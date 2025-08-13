from typing import List, Tuple, Dict
import uuid
from .executor import Executor
# from .pipeline import Pipeline, Operator
from eudoxia.utils import Suspend, Assignment, Failure
from eudoxia.utils import Priority, Pipeline, Operator
from eudoxia.algorithms import INIT_ALGOS, SCHEDULING_ALGOS
import logging
logger = logging.getLogger(__name__)

class Scheduler:
    """
    A modular class which can utilize any implemented scheduling algorithm. It
    accepts pipelines from the WorkloadGenerator and constructs assignments
    based on the algorithm its utilizing. It provides these assignments to the
    Executor.
    """

    # ''' The table of pre-implemented scheduling algorithms and corresponding
    # initialization functions'''
    def __init__(self, executor: Executor, scheduler_algo):
        self.executor = executor
        if not scheduler_algo in SCHEDULING_ALGOS:
            options = sorted(SCHEDULING_ALGOS.keys())
            raise ValueError(f"Scheduler {repr(scheduler_algo)} not available.  Options: {repr(options)}'")
        self.algo_name = scheduler_algo
        self.algo_func = SCHEDULING_ALGOS[scheduler_algo]
        logger.info(scheduler_algo)
        INIT_ALGOS[scheduler_algo](self)

    def run_one_tick(self, failures: List[Failure], 
                     pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
        suspensions, assignments = self.algo_func(self, failures, pipelines)
        return (suspensions, assignments)

