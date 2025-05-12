import tomllib
import logging
from typing import List, Dict
import numpy as np

from eudoxia.utils.consts import TICK_LENGTH_SECS
from eudoxia.core import WorkloadGenerator, Scheduler, Executor 
from eudoxia.utils import Assignment, Pipeline

logger = logging.getLogger(__name__)

__all__ = ["run_simulator", "parse_args_with_defaults"]

def parse_args_with_defaults(params: Dict) -> Dict:
    """
    parses the passed in parameters and fills in default values;

    Args:
        params (dict): params read from TOML file
    Returns:
        dict: params plus default values for any values not supplied

    """
    # how long the simulation will run in seconds
    if "duration" not in params:
        params["duration"] = 60

    ###
    ### Workload Generation Params
    ###
    # how many ticks the dispatcher will wait between generating pipelines
    if "waiting_ticks_mean" not in params:
        params["waiting_ticks_mean"] = 1,000,000
    # number of pipelines to generate when pipelines are created
    if "num_pipelines" not in params:
        params["num_pipelines"] = 4
    # average number of operators per pipeline
    if "num_operators" not in params:
        params["num_operators"] = 5
    # NOT IN USE (Aug 15 '24); how many disjoint subtrees there are in a
    # pipeline, i.e. how parallelizable the pipeline is
    if "parallel_factor" not in params:
        params["parallel_factor"] = 1
    # num_segs not being used: all operators have a single segment
    if "num_segs" not in params:
        params["num_segs"] = 1
    # probabilities for different pipeline priority levels
    if "interactive_prob" not in params:
        params["interactive_prob"] = 0.3
    if "query_prob" not in params:
        params["query_prob"] = 0.1
    if "batch_prob" not in params:
        params["batch_prob"] = 0.6
    assert (params["interactive_prob"] + params["query_prob"] + params["batch_prob"] == 1)
    # value between 0 and 1 indicating on average how IO heavy a segment is. Low
    # value is IO heavier
    if "cpu_io_ratio" not in params:
        params["cpu_io_ratio"] = 0.5
    else:
        assert params["cpu_io_ratio"] <= 1 and params["cpu_io_ratio"] >= 0, \
               "CPU IO ratio must be between 0 and 1"

    ###
    ### Scheduler Params
    ###
    if "scheduler_algo" not in params:
        params["scheduler_algo"] = "priority"

    ###
    ### Executor Params
    ###
    # number of resource pools for executors
    if "num_pools" not in params:
        params["num_pools"] = 1
    # number of CPUs or vCPUs 
    if "cpu_pool" not in params:
        params["cpu_pool"] = 64
    # GB in RAM pool
    if "ram_pool" not in params:
        params["ram_pool"] = 500
    # random seed and rng generator
    if "random_seed" not in params:
        params["random_seed"] = 10
    params["rng"] = np.random.default_rng(params["random_seed"])
    return params

def run_simulator(param_file: str):
    """
    The main method to run the simulator. There are three core entities,
    WorkloadGenerator, Scheduler, and Executor. Each offers a `run_one_tick`
    function. This function encodes the core event loop which runs one tick per
    iteration and handles that by running each of the three in sequence, passing
    the results of one function to another

    Args:
        param_file: location of TOML file with params
    """
    try: 
        with open(param_file, 'rb') as fp:
            params = tomllib.load(fp)
    except FileNotFoundError:
        logger.error(f"Invalid parameter file provided")
        raise
    except tomllib.TOMLDecodeError:
        logger.error("Error parsing param file: should be in TOML format")
        raise

    # Sanitize parameters and fill in default values
    params = parse_args_with_defaults(params)

    # INITIALIZATION
    work_gen = WorkloadGenerator(**params)
    executor = Executor(**params)
    scheduler = Scheduler(executor, scheduler_algo=params["scheduler_algo"])

    tick_number = 0
    max_ticks = int(params["duration"] / TICK_LENGTH_SECS)
    logger.info(f"Running for {params['duration']}s or {max_ticks} ticks")
    logger.info(f"Running with random seed {params['random_seed']}")

    num_pipelines_created = 0
    failures = []
    while tick_number < max_ticks:
        pipelines: List[Pipeline] = work_gen.run_one_tick()
        num_pipelines_created += len(pipelines)
        suspensions, assignments = scheduler.run_one_tick(failures, pipelines)
        failures = executor.run_one_tick(suspensions, assignments)

        tick_number += 1

    # TODO: better way to calculate work thruput, going by num ops, etc. is
    # going to skew towards more smaller jobs
    oom = scheduler.oom_failed_to_run 
    thruput = executor.num_completed() / params['duration']
    p99 = np.percentile(executor.container_tick_times(), 99) * TICK_LENGTH_SECS
    logger.info(f"Ran for {params['duration']} seconds or {max_ticks} ticks")
    logger.info(f"Created {num_pipelines_created} pipelines")
    logger.info(f"{oom} pipelines couldn't run w/o using >50% of system RAM")
    logger.info(f"Completed {executor.num_completed()} pipelines with a thruput of {thruput}")
    logger.info(f"{p99:.2f}s p99 latency")





