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
    scheduler = Scheduler(executor, scheduler_algo=scheduler_algo, multi_operator_containers=True)

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
    scheduler = Scheduler(executor, scheduler_algo=scheduler_algo, multi_operator_containers=True)

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
        pool=executor.pools[pool_id],
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


def test_op_not_double_queued_across_ticks():
    """Ops in queue but not yet assigned should not be queued again when another result arrives."""
    executor = Executor(
        num_pools=1,
        cpus_per_pool=30,
        ram_gb_per_pool=30,
        ticks_per_second=10,
        multi_operator_containers=False,
    )
    scheduler = Scheduler(executor, scheduler_algo="priority", multi_operator_containers=False)

    # Pipeline: op1 has 20 children, op2 is independent
    pipeline = Pipeline("test", Priority.BATCH_PIPELINE)
    op1 = pipeline.new_operator()
    op1.add_segment(Segment(baseline_cpu_seconds=0.1, memory_gb=5))
    op2 = pipeline.new_operator()
    op2.add_segment(Segment(baseline_cpu_seconds=0.1, memory_gb=5))
    children = []
    for i in range(20):
        c = pipeline.new_operator([op1])
        c.add_segment(Segment(baseline_cpu_seconds=0.1, memory_gb=5))
        children.append(c)

    # Simulate op1 and op2 running externally
    op1.transition(OperatorState.ASSIGNED)
    op1.transition(OperatorState.RUNNING)
    op2.transition(OperatorState.ASSIGNED)
    op2.transition(OperatorState.RUNNING)

    # op1 completes first, all 20 children become ready
    op1.transition(OperatorState.COMPLETED)
    success1 = ExecutionResult(
        ops=[op1], cpu=10, ram=10,
        priority=Priority.BATCH_PIPELINE, pool_id=0, error=None
    )

    # Tick 1: op1 result arrives, children get queued, not all fit due to resources
    suspensions, assignments = scheduler.run_one_tick([success1], [])
    first_assigned = len(assignments)
    assert first_assigned < 20, "not all children should be assigned (resource limit)"
    remaining_in_queue = len(scheduler.batch_ppln_jobs)
    assert remaining_in_queue > 0, "some children should remain in queue"

    # Actually consume resources by passing assignments to executor
    executor.run_one_tick([], assignments)

    # op2 completes
    op2.transition(OperatorState.COMPLETED)
    success2 = ExecutionResult(
        ops=[op2], cpu=10, ram=10,
        priority=Priority.BATCH_PIPELINE, pool_id=0, error=None
    )

    # Tick 2: op2 result arrives, but remaining children should NOT be queued again
    suspensions, assignments = scheduler.run_one_tick([success2], [])
    assert len(scheduler.batch_ppln_jobs) == remaining_in_queue, "children should not be double-queued"


def test_failed_op_gets_retry_stats():
    """A failed op should be queued with retry stats, not as a fresh job."""
    executor = Executor(
        num_pools=1,
        cpus_per_pool=50,
        ram_gb_per_pool=50,
        ticks_per_second=10,
        multi_operator_containers=False,
    )
    scheduler = Scheduler(executor, scheduler_algo="priority", multi_operator_containers=False)

    # Single op pipeline with memory requirement higher than initial allocation will provide
    # Scheduler gives 10% of pool (5 GB), but op needs 10 GB -> OOM
    # Use enough CPU time so the container has multiple ticks (OOM detected on first tick)
    pipeline = Pipeline("test", Priority.BATCH_PIPELINE)
    op1 = pipeline.new_operator()
    op1.add_segment(Segment(baseline_cpu_seconds=1.0, memory_gb=10))

    # First tick: pipeline arrives, op1 gets assigned
    suspensions, assignments = scheduler.run_one_tick([], [pipeline])
    assert len(assignments) == 1
    first_cpu = assignments[0].cpu
    first_ram = assignments[0].ram

    # Run executor - op will OOM because it needs 10GB but only got 5GB
    results = executor.run_one_tick([], assignments)
    assert len(results) == 1
    assert results[0].error == "OOM"

    # Second tick: failure comes back, op1 should be assigned with doubled resources
    suspensions, assignments = scheduler.run_one_tick(results, [])
    assert len(assignments) == 1, "failed op should be assigned exactly once"
    assert assignments[0].cpu == 2 * first_cpu, "retry should get doubled CPU"
    assert assignments[0].ram == 2 * first_ram, "retry should get doubled RAM"


def test_partial_failure_unblocks_dependent_op():
    """When op1 completes but op2 fails in same container, op3 (dependent on op1) should be queued."""
    executor = Executor(
        num_pools=1,
        cpus_per_pool=50,
        ram_gb_per_pool=50,
        ticks_per_second=10,
        multi_operator_containers=True,
    )
    scheduler = Scheduler(executor, scheduler_algo="priority", multi_operator_containers=True)

    # Pipeline: op1 (small memory) and op2 (will OOM) run together, op3 depends on op1
    #   op1 -> op3
    #   op2 (OOMs)
    # Container gets 5GB (10% of pool). op1 needs 1GB (ok), op2 needs 10GB (OOM)
    pipeline = Pipeline("test", Priority.BATCH_PIPELINE)
    op1 = pipeline.new_operator()
    op1.add_segment(Segment(baseline_cpu_seconds=1, memory_gb=1))  # needs 1GB, fits in 5GB
    op2 = pipeline.new_operator()
    op2.add_segment(Segment(baseline_cpu_seconds=1, memory_gb=10))  # needs 10GB, OOMs with 5GB
    op3 = pipeline.new_operator([op1])
    op3.add_segment(Segment(baseline_cpu_seconds=1, memory_gb=1))

    # First tick: pipeline arrives, all 3 ops get assigned together
    suspensions, assignments = scheduler.run_one_tick([], [pipeline])
    assert len(assignments) == 1
    assert set(assignments[0].ops) == {op1, op2, op3}

    # Run executor until we get a result (op1 should complete, then op2 OOMs)
    results = executor.run_one_tick([], assignments)
    while not results:
        results = executor.run_one_tick([], [])
    assert len(results) == 1
    assert results[0].error == "OOM"
    assert op1.state() == OperatorState.COMPLETED, "op1 should have completed before OOM"

    # Second tick: op2 and op3 should be retried (op1 already completed)
    suspensions, assignments = scheduler.run_one_tick(results, [])
    assert len(assignments) == 1
    assigned_ops = set(assignments[0].ops)
    assert op1 not in assigned_ops, "completed op1 should not be retried"
    assert op2 in assigned_ops, "failed op2 should be retried"
    assert op3 in assigned_ops, "op3 should be retried (was in failed container)"
