from typing import List, Tuple
from eudoxia.workload import Pipeline, OperatorState
from eudoxia.workload.runtime_status import ASSIGNABLE_STATES
from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
from eudoxia.utils import Priority
from .decorators import register_scheduler_init, register_scheduler


@register_scheduler_init(key="naive")
def naive_pipeline_init(s):
    s.waiting_queue: List[Pipeline] = []
    s.multi_operator_containers = s.params["multi_operator_containers"]

@register_scheduler(key="naive")
def naive_pipeline(s, results: List[ExecutionResult],
                   pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    A naive implementation of a scheduling pipeline which allocates all
    resources of a pool to a single pipeline and handles them in a FIFO order. It
    assigns one job at a time to each pool created.
    Args:
        results: List of execution results from previous tick (ignored in naive implementation)
        pipelines: List of pipelines from WorkloadGenerator
    Returns:
        Tuple[List[Suspend], List[Assignment]]:
            - List of containers to suspend (always empty for naive scheduler)
            - List of new assignments to provide to Executor
    """
    for p in pipelines:
        s.waiting_queue.append(p)

    suspensions = []
    assignments = []

    # we usually requeue because there might be more ops that we can't
    # run initially.  we use this separate structure (instead of
    # immediately requeing) to avoid infinite looping over queues.
    requeue_pipelines = []

    for pool_id in range(s.executor.num_pools):
        # skip pools without available capacity
        avail_cpu_pool = s.executor.pools[pool_id].avail_cpu_pool
        avail_ram_pool = s.executor.pools[pool_id].avail_ram_pool
        if avail_cpu_pool <= 0 or avail_ram_pool <= 0:
            continue

        # find a pipeline with ops we can assign
        while s.waiting_queue:
            pipeline = s.waiting_queue.pop(0)
            has_failures = pipeline.runtime_status().state_counts[OperatorState.FAILED] > 0
            if pipeline.runtime_status().is_pipeline_successful() or has_failures:
                # we don't retry, so anything complete or with failures
                # will be permanently removed from the queue
                continue

            # might be more work for later, after whatever we might do this round
            requeue_pipelines.append(pipeline)

            # if our containers can run multiple ops, we send all assignable ops;
            # otherwise, just the first assignable op with parents complete
            if s.multi_operator_containers:
                op_list = pipeline.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
            else:
                op_list = pipeline.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue  # might be ready later

            # assign all resources of this pool/machine.  break out of
            # the inner loop, because we will therefore not be able to
            # assign more pipelines to this pool (but maybe more
            # pipelines to other pools, in the outer loop)
            assignment = Assignment(ops=op_list, cpu=avail_cpu_pool, ram=avail_ram_pool,
                                    priority=pipeline.priority, pool_id=pool_id,
                                    pipeline_id=pipeline.pipeline_id)
            assignments.append(assignment)
            break

    s.waiting_queue.extend(requeue_pipelines)
    return suspensions, assignments
