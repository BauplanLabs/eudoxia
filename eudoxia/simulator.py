import tomllib
import logging
from collections import defaultdict
from typing import List, Dict, Union, NamedTuple
import numpy as np

from eudoxia.utils.consts import MICROSEC_TO_SEC
from eudoxia.executor import Executor
from eudoxia.scheduler import Scheduler
from eudoxia.workload import Workload, WorkloadGenerator, Pipeline
from eudoxia.executor.assignment import Assignment

__all__ = ["run_simulator", "parse_args_with_defaults", "get_param_defaults"]

logger = logging.getLogger(__name__)

class SimulatedTimeFormatter(logging.Formatter):
    """Custom formatter that adds elapsed simulation time to log
    messages.  Unlike normal logging formats based on wall-clock time,
    we want to print elapsed simulator time."""
    def __init__(self):
        self.elapsed_seconds = 0.0

    def set_simulated_elapsed_seconds(self, seconds):
        self.elapsed_seconds = seconds

    def format(self, record):
        # Format: [elapsed_time] LEVEL:logger_name: message
        return f"[{self.elapsed_seconds:6.1f}s] {record.levelname}:{record.name}: {record.getMessage()}"

class SimulatorStats(NamedTuple):
    """Statistics returned from a simulator run"""
    pipelines_created: int
    pipelines_completed: int
    throughput: float
    p99_latency: float
    assignments: int
    suspensions: int
    failures: int
    failure_error_counts: int

def get_param_defaults() -> Dict:
    """
    Returns a dictionary with all default parameter values.
    
    Returns:
        dict: Default values for all simulator parameters
    """
    return {
        # how long the simulation will run in seconds
        "duration": 60,
        # number of ticks per second (100,000 ticks per second by default = 10 microseconds per tick)
        "ticks_per_second": 100_000,
        
        ### Workload Generation Params ###
        # how many seconds on average the dispatcher will wait between generating pipelines
        "waiting_seconds_mean": 10.0,
        # number of pipelines to generate when pipelines are created
        "num_pipelines": 4,
        # average number of operators per pipeline
        "num_operators": 5,
        # num_segs not being used: all operators have a single segment
        "num_segs": 1,
        # probabilities for different pipeline priority levels
        "interactive_prob": 0.3,
        "query_prob": 0.1,
        "batch_prob": 0.6,
        # value between 0 and 1 indicating on average how IO heavy a segment is. Low
        # value is IO heavier
        "cpu_io_ratio": 0.5,
        
        ### Scheduler Params ###
        "scheduler_algo": "priority",
        
        ### Executor Params ###
        # number of resource pools for executors
        "num_pools": 1,
        # number of CPUs or vCPUs
        "cpu_pool": 64,
        # GB in RAM pool
        "ram_pool": 500,
        # random seed for workload generation
        "random_seed": 10,
    }

def parse_args_with_defaults(params: Dict) -> Dict:
    """
    parses the passed in parameters and fills in default values;

    Args:
        params (dict): params read from TOML file
    Returns:
        dict: params plus default values for any values not supplied

    """
    # Copy the input params to avoid modifying the original
    result = params.copy()

    # Get defaults and add any missing values
    defaults = get_param_defaults()
    for key, default_value in defaults.items():
        if key not in result:
            result[key] = default_value

    return result

def run_simulator(param_input: Union[str, Dict], workload: Workload = None) -> SimulatorStats:
    """
    The main method to run the simulator. There are three core entities,
    WorkloadGenerator, Scheduler, and Executor. Each offers a `run_one_tick`
    function. This function encodes the core event loop which runs one tick per
    iteration and handles that by running each of the three in sequence, passing
    the results of one function to another

    Args:
        param_input: Either a path to a TOML file with params, or a dict of params
        workload: Optional custom workload source. If None, uses WorkloadGenerator with params
    
    Returns:
        SimulatorStats: Statistics from the simulation run including pipelines
        created/completed, OOM failures, throughput, and p99 latency
    """
    # Load params from file or use dict directly
    if isinstance(param_input, str):
        try: 
            with open(param_input, 'rb') as fp:
                params = tomllib.load(fp)
        except FileNotFoundError:
            logger.error(f"Invalid parameter file provided")
            raise
        except tomllib.TOMLDecodeError:
            logger.error("Error parsing param file: should be in TOML format")
            raise
    else:
        params = param_input.copy()

    # Sanitize parameters and fill in default values
    params = parse_args_with_defaults(params)
    
    # Validate constraints
    assert (params["interactive_prob"] + params["query_prob"] + params["batch_prob"] == 1), \
        "Probabilities must sum to 1"
    assert params["cpu_io_ratio"] <= 1 and params["cpu_io_ratio"] >= 0, \
        "CPU IO ratio must be between 0 and 1"
    
    # INITIALIZATION
    if workload is None:
        workload = WorkloadGenerator(**params)

    executor = Executor(**params)
    scheduler = Scheduler(executor, scheduler_algo=params["scheduler_algo"])

    # Set up custom logging with elapsed time
    ticks_per_second = params["ticks_per_second"]

    # Configure log handlers to use simulated time (instead of real time)
    sim_formatter = SimulatedTimeFormatter()
    for handler in logging.getLogger().handlers:
        handler.setFormatter(sim_formatter)

    tick_number = 0
    max_ticks = int(params["duration"] * params["ticks_per_second"])
    logger.info(f"Running for {params['duration']}s or {max_ticks} ticks")
    logger.info(f"Running with random seed {params['random_seed']}")

    # a pipeline may have many operators.  These can get grouped
    # together into some number of containers, which are assigned to
    # run on machines (that is, resource pools).  These containers
    # may fail or be suspended.
    num_pipelines_created = 0
    num_assignments = 0
    num_suspenions = 0
    num_failures = 0
    failure_error_counts = defaultdict(int)
    executor_failures = []

    # IMPORTANT!  This is the main simulation loop.
    for tick_number in range(max_ticks):
        sim_formatter.set_simulated_elapsed_seconds(tick_number / ticks_per_second)

        new_pipelines: List[Pipeline] = workload.run_one_tick()
        for p in new_pipelines:
            logger.info(f"Pipeline arrived with Priority {p.priority} and {len(p.values)} op(s)")
        suspensions, assignments = scheduler.run_one_tick(executor_failures, new_pipelines)
        executor_failures = executor.run_one_tick(suspensions, assignments)

        # track stats
        num_pipelines_created += len(new_pipelines)
        num_assignments += len(assignments)
        num_suspenions += len(suspensions)
        num_failures += len(executor_failures)
        for failure in executor_failures:
            failure_error_counts[failure.error] += 1

    # TODO: better way to calculate work throuphput, going by num ops, etc. is
    # going to skew towards more smaller jobs
    throughput = executor.num_completed() / params['duration']
    p99 = np.percentile(executor.container_tick_times(), 99) / params["ticks_per_second"]

    return SimulatorStats(
        pipelines_created=num_pipelines_created,
        pipelines_completed=executor.num_completed(),
        throughput=throughput,
        p99_latency=p99,
        assignments=num_assignments,
        suspensions=num_suspenions,
        failures=num_failures,
        failure_error_counts=dict(failure_error_counts),
    )
