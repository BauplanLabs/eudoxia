import pytest
from eudoxia.scheduler import Scheduler
from eudoxia.executor import Executor
from eudoxia.executor.container import Container
from eudoxia.executor.assignment import ExecutionResult
from eudoxia.workload import Pipeline, OperatorState
from eudoxia.workload.pipeline import Segment
from eudoxia.utils import Priority


@pytest.mark.parametrize("scheduler_algo", ["priority", "priority-pool"])
def test_retry_only_assigns_incomplete_operators(scheduler_algo):
    """When a container OOMs, only incomplete operators should be retried."""
    executor = Executor(
        num_pools=2,
        cpus_per_pool=50,
        ram_gb_per_pool=500,
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
    # (ops already transitioned to ASSIGNED when Assignment was created)
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


@pytest.mark.parametrize("scheduler_algo", ["priority", "priority-pool"])
def test_resume_only_assigns_incomplete_operators(scheduler_algo):
    """When a container resumes after suspension, only incomplete operators should be assigned."""
    executor = Executor(
        num_pools=2,
        cpus_per_pool=50,
        ram_gb_per_pool=500,
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
    first_assignment = assignments[0]
    pool_id = first_assignment.pool_id

    # Create a container from the assignment (ops already transitioned to ASSIGNED)
    container = Container(
        assignment=first_assignment,
        pool_id=pool_id,
        ticks_per_second=10,
    )

    # Simulate: op1 runs and completes, op2 stays assigned, then container is suspended
    op1.transition(OperatorState.RUNNING)
    op1.transition(OperatorState.COMPLETED)
    op2.transition(OperatorState.SUSPENDING)

    # Put container in suspending state
    container._is_suspending = True
    container.suspend_ticks = 1
    executor.pools[pool_id].suspending_containers.append(container)

    # Scheduler sees suspending container, stores job
    suspensions, assignments = scheduler.run_one_tick([], [])

    # Move container to suspended state (op2 transitions to PENDING when suspension completes)
    executor.pools[pool_id].suspending_containers.remove(container)
    container._is_suspending = False
    container._is_suspended = True
    op2.transition(OperatorState.PENDING)  # Suspension complete, ready to be re-assigned
    executor.pools[pool_id].suspended_containers.append(container)

    # Scheduler sees suspended container, should only assign incomplete op2
    suspensions, assignments = scheduler.run_one_tick([], [])
    assert len(assignments) == 1
    assert assignments[0].ops == [op2], "should only assign incomplete operator"
