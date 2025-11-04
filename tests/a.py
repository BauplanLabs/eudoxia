from eudoxia.scheduler import register_scheduler_init, register_scheduler
from eudoxia.utils import *
from eudoxia.workload import Pipeline, Operator
from eudoxia.executor.assignment import Assignment, Suspend, ExecutionResult
from typing import Tuple, List


@register_scheduler_init(key="my")
def my_init(s):
    s.waiting_queue: Tuple[List[Operator], Priority] = []

@register_scheduler(key="my")
def my_scheduler(s, results: List[ExecutionResult],
                   pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    A naive implementation of a scheduling pipeline which allocates all
    resources of a pool to a single pipeline and handles them in a FIFO order. It
    assigns one job at a time to each pool created.
    Args:
        results: List of execution results from previous tick (ignored in this implementation)
        pipelines: List of pipelines from WorkloadGenerator
    Returns:
        Tuple[List[Suspend], List[Assignment]]:
            - List of containers to suspend (always empty for this scheduler)
            - List of new assignments to provide to Executor
    """
    for p in pipelines:
        ops = [op for op in p.values]
        s.waiting_queue.append((ops, p.priority))
    if len(s.waiting_queue) == 0:
        return [], []

    suspensions = []
    assignments = []
    for pool_id in range(s.executor.num_pools):
        avail_cpu_pool = s.executor.pools[pool_id].avail_cpu_pool
        avail_ram_pool = s.executor.pools[pool_id].avail_ram_pool
        if avail_cpu_pool > 0 and avail_ram_pool > 0 and s.waiting_queue:
            op_list, priority = s.waiting_queue.pop(0)
            assignment = Assignment(ops=op_list, cpu=avail_cpu_pool, ram=avail_ram_pool,
                                    priority=priority, pool_id=pool_id)
            assignments.append(assignment)
    return suspensions, assignments

