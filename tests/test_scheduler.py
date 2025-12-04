import pytest
from eudoxia.scheduler import Scheduler
from eudoxia.executor import Executor
from eudoxia.executor.assignment import ExecutionResult
from eudoxia.workload import Pipeline, OperatorState
from eudoxia.workload.pipeline import Segment
from eudoxia.utils import Priority


@pytest.mark.parametrize("scheduler_algo", ["priority", "priority-pool"])
def test_retry_only_assigns_incomplete_operators(scheduler_algo):
    """When a container OOMs, only incomplete operators should be retried."""
    executor = Executor(
        num_pools=2,
        cpu_pool=100,
        ram_pool=1000,
        ticks_per_second=10,
        multi_operator_containers=True,
    )
    scheduler = Scheduler(executor, scheduler_algo=scheduler_algo)

    # Pipeline with 2 ops
    pipeline = Pipeline("test", Priority.BATCH_PIPELINE)
    op1 = pipeline.new_operator()
    op1.add_segment(Segment(baseline_cpu_seconds=0.1, memory_gb=5))
    op2 = pipeline.new_operator([op1])
    op2.add_segment(Segment(baseline_cpu_seconds=0.1, memory_gb=5))

    # First call: new pipeline arrives
    suspensions, assignments = scheduler.run_one_tick([], [pipeline])
    assert len(assignments) == 1
    assert set(assignments[0].ops) == {op1, op2}
    first_assignment = assignments[0]

    # Simulate: op1 completes, then container fails
    op1.transition(OperatorState.ASSIGNED)
    op2.transition(OperatorState.ASSIGNED)
    op1.transition(OperatorState.RUNNING)
    op1.transition(OperatorState.COMPLETED)
    op2.transition(OperatorState.FAILED)

    # Report failure with both ops (use resources from first assignment)
    failure = ExecutionResult(
        ops=[op1, op2],
        cpu=first_assignment.cpu,
        ram=first_assignment.ram,
        priority=Priority.BATCH_PIPELINE,
        pool_id=first_assignment.pool_id,
        error="OOM"
    )

    # Second call: failure comes back, should only retry op2
    suspensions, assignments = scheduler.run_one_tick([failure], [])
    assert len(assignments) == 1
    assert assignments[0].ops == [op2], "should only assign incomplete operator"
