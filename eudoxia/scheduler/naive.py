from typing import List, Tuple
from eudoxia.workload import Pipeline, Operator, Assignment, Failure, Suspend
from eudoxia.utils import Priority
from .decorators import register_scheduler_init, register_scheduler


@register_scheduler_init(key="naive")
def naive_pipeline_init(s): 
    s.waiting_queue: Tuple[List[Operator], Priority] = []

@register_scheduler(key="naive")
def naive_pipeline(s, failures: List[Failure], 
                   pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    A naive implementation of a scheduling pipeline which allocates all
    resources of a pool to a single pipeline and handles them in a FIFO order. It
    assigns one job at a time to each pool created.
    Args:
        failures: List of failed containers from previous tick (ignored in naive implementation)
        pipelines: List of pipelines from WorkloadGenerator
    Returns:
        Tuple[List[Suspend], List[Assignment]]: 
            - List of containers to suspend (always empty for naive scheduler)
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