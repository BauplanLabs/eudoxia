from collections import defaultdict
from typing import List, Tuple, Dict
import logging
from eudoxia.workload import Pipeline, Operator, OperatorState
from eudoxia.workload.runtime_status import ASSIGNABLE_STATES
from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
from .decorators import register_scheduler_init, register_scheduler

logger = logging.getLogger(__name__)

MAX_FAILURES = 3  # give up on pipeline after 3 op failures


@register_scheduler_init(key="overbook")
def overbook_init(s):
    """Initialize the overbook scheduler.

    This scheduler demonstrates memory overcommit by allocating full pool RAM
    to each container while only respecting CPU limits.
    """
    s.op_queue: List[Operator] = []
    s.pipeline_failures: Dict[str, int] = defaultdict(int)  # pipeline_id -> failure count
    s.avail_cpus: Dict[int, int] = {}  # pool_id -> unallocated CPUs

    if not s.params.get("allow_memory_overcommit", False):
        logger.warning(
            "overbook scheduler: allow_memory_overcommit is not enabled. "
            "This scheduler is designed to demonstrate memory overcommit and may "
            "fail assertions or behave unexpectedly without it."
        )


@register_scheduler(key="overbook")
def overbook_scheduler(s, results: List[ExecutionResult],
                       pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    A scheduler that demonstrates memory overcommit.

    - Each operator runs in its own container with 1 CPU
    - Memory allocation equals the full pool RAM (ignoring availability)
    - Only CPU availability is checked before assignment
    - Pipelines are abandoned after 3 total op failures
    - No suspensions, no priority handling
    """
    if not pipelines and not results:
        return [], []

    update_state(s, results, pipelines)
    assignments = make_assignments(s)
    return [], assignments


def update_state(s, results: List[ExecutionResult], pipelines: List[Pipeline]):
    """Update scheduler state based on new results and pipeline arrivals."""

    # Collect all pipelines needing attention: new arrivals + those with results
    pipelines_to_process = {p.pipeline_id: p for p in pipelines}
    
    for r in results:
        pipeline = only(r.ops).pipeline
        pipelines_to_process[pipeline.pipeline_id] = pipeline
        if r.failed():
            s.pipeline_failures[pipeline.pipeline_id] += 1

    # Queue ready ops from all pipelines needing attention
    queued_ids = {op.id for op in s.op_queue}
    for pipeline in pipelines_to_process.values():
        ready_ops = pipeline.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        for op in ready_ops:
            if op.id in queued_ids:
                continue # suppress duplicates
            s.op_queue.append(op)
            queued_ids.add(op.id)

    # Snapshot pool availability
    s.avail_cpus = {pool.pool_id: pool.avail_cpu_pool for pool in s.executor.pools}


def try_make_assignment(s, op: Operator) -> Assignment | None:
    """Try to assign an op to a pool with available CPU. Returns None if not possible."""
    for pool_id, avail in s.avail_cpus.items():
        if avail >= 1:
            pool = s.executor.pools[pool_id]
            s.avail_cpus[pool_id] -= 1
            return Assignment(
                ops=[op],
                cpu=1,
                ram=pool.max_ram_pool,
                priority=op.pipeline.priority,
                pool_id=pool_id,
                pipeline_id=op.pipeline.pipeline_id
            )
    return None


def make_assignments(s) -> List[Assignment]:
    """Assign queued ops to pools with available CPU."""
    assignments = []

    for op_idx, op in enumerate(s.op_queue):
        if s.pipeline_failures[op.pipeline.pipeline_id] >= MAX_FAILURES:
            continue
        assert op.state() in ASSIGNABLE_STATES, f"op {op.id} of pipeline {op.pipeline.pipeline_id} was in queue, but has non-assignable state, {op.state()}"

        assignment = try_make_assignment(s, op)
        if not assignment:
            # no more capacity
            s.op_queue = s.op_queue[op_idx:]
            return assignments

        assignments.append(assignment)

    s.op_queue = [] # if we didn't return early, it means we dispatched everything
    return assignments


def only(iterable):
    [item] = iterable
    return item
