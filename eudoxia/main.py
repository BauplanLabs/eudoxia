import tomllib
import logging
from typing import List, Dict, Union, NamedTuple
import numpy as np

from eudoxia.utils.consts import TICK_LENGTH_SECS
from eudoxia.core import Workload, WorkloadGenerator, Scheduler, Executor 
from eudoxia.utils import Assignment, Pipeline


class SimulatorStats(NamedTuple):
    """Statistics returned from a simulator run"""
    pipelines_created: int
    pipelines_completed: int
    oom_failures: int
    throughput: float
    p99_latency: float

logger = logging.getLogger(__name__)

__all__ = ["run_simulator", "parse_args_with_defaults", "get_param_defaults"]

def get_param_defaults() -> Dict:
    """
    Returns a dictionary with all default parameter values.
    
    Returns:
        dict: Default values for all simulator parameters
    """
    return {
        # how long the simulation will run in seconds
        "duration": 60,
        
        ### Workload Generation Params ###
        # how many ticks the dispatcher will wait between generating pipelines
        "waiting_ticks_mean": 1_000_000,
        # number of pipelines to generate when pipelines are created
        "num_pipelines": 4,
        # average number of operators per pipeline
        "num_operators": 5,
        # NOT IN USE (Aug 15 '24); how many disjoint subtrees there are in a
        # pipeline, i.e. how parallelizable the pipeline is
        "parallel_factor": 1,
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
        # random seed and rng generator
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
    
    if not "rng" in params:
        params["rng"] = np.random.default_rng(params["random_seed"])

    # INITIALIZATION
    if workload is None:
        workload = WorkloadGenerator(**params)
    executor = Executor(**params)
    scheduler = Scheduler(executor, scheduler_algo=params["scheduler_algo"])

    tick_number = 0
    max_ticks = int(params["duration"] / TICK_LENGTH_SECS)
    logger.info(f"Running for {params['duration']}s or {max_ticks} ticks")
    logger.info(f"Running with random seed {params['random_seed']}")

    num_pipelines_created = 0
    failures = []
    while tick_number < max_ticks:
        pipelines: List[Pipeline] = workload.run_one_tick()
        num_pipelines_created += len(pipelines)
        suspensions, assignments = scheduler.run_one_tick(failures, pipelines)
        failures = executor.run_one_tick(suspensions, assignments)

        tick_number += 1

    # TODO: better way to calculate work thruput, going by num ops, etc. is
    # going to skew towards more smaller jobs
    oom = getattr(scheduler, 'oom_failed_to_run', 0) 
    thruput = executor.num_completed() / params['duration']
    p99 = np.percentile(executor.container_tick_times(), 99) * TICK_LENGTH_SECS
    logger.info(f"Ran for {params['duration']} seconds or {max_ticks} ticks")
    logger.info(f"Created {num_pipelines_created} pipelines")
    logger.info(f"{oom} pipelines couldn't run w/o using >50% of system RAM")
    logger.info(f"Completed {executor.num_completed()} pipelines with a thruput of {thruput}")
    logger.info(f"{p99:.2f}s p99 latency")
    
    return SimulatorStats(
        pipelines_created=num_pipelines_created,
        pipelines_completed=executor.num_completed(),
        oom_failures=oom,
        throughput=thruput,
        p99_latency=p99
    )
