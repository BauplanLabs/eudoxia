import pytest
from eudoxia.executor import Executor
from eudoxia.executor.resource_pool import ResourcePool
from eudoxia.executor.assignment import Assignment, Suspend
from eudoxia.workload.pipeline import Segment, Operator, Pipeline
from eudoxia.workload import OperatorState
from eudoxia.utils import Priority, DISK_SCAN_GB_SEC


def test_resource_pool_basic():
    """
    Test that resource pool returns both successful and failed ExecutionResults.
    Create two assignments: one that will succeed and one that will OOM.
    """
    # Create resource pool with limited resources
    pool = ResourcePool(
        pool_id=0,
        cpu_pool=10,
        ram_pool=100,
        ticks_per_second=10,
        multi_operator_containers=True
    )

    # Create pipeline and operator for successful container
    pipeline_success = Pipeline(pipeline_id="success_pipeline", priority=Priority.BATCH_PIPELINE)
    op_success = pipeline_success.new_operator()
    op_success.add_segment(Segment(
        baseline_cpu_seconds=0.1,  # 0.1 seconds
        cpu_scaling="const",
        memory_gb=10,  # Reasonable memory usage
        storage_read_gb=0
    ))

    # Create pipeline and operator for OOM container
    # Use storage_read_gb with no fixed memory_gb to get linear growth OOM
    pipeline_oom = Pipeline(pipeline_id="oom_pipeline", priority=Priority.BATCH_PIPELINE)
    op_oom = pipeline_oom.new_operator()
    op_oom.add_segment(Segment(
        baseline_cpu_seconds=0.1,  # 0.1 seconds
        cpu_scaling="const",
        storage_read_gb=200  # Linear memory growth - will OOM when it hits 50GB limit
        # memory_gb not set, so memory grows linearly as storage is read
    ))

    # Create assignments
    assignment_success = Assignment(
        ops=[op_success],
        cpu=5,
        ram=50,  # Enough for the segment (needs 10GB)
        priority=Priority.BATCH_PIPELINE,
        pool_id=0,
        pipeline_id="success_pipeline"
    )

    assignment_oom = Assignment(
        ops=[op_oom],
        cpu=5,
        ram=50,  # Not enough for the segment (needs 200GB)
        priority=Priority.BATCH_PIPELINE,
        pool_id=0,
        pipeline_id="oom_pipeline"
    )

    assignments = [assignment_success, assignment_oom]
    suspensions = []

    # Run ticks until both containers complete
    max_ticks = 1000
    results_by_pipeline = {}

    for tick in range(max_ticks):
        results = pool.run_one_tick(suspensions, assignments)

        # Organize results by pipeline_id
        for result in results:
            pipeline_id = result.ops[0].pipeline.pipeline_id
            results_by_pipeline[pipeline_id] = result

        # Only provide assignments on first tick
        assignments = []

    # Verify only success_pipeline succeeded
    success_result = results_by_pipeline["success_pipeline"]
    oom_result = results_by_pipeline["oom_pipeline"]
    assert not success_result.failed(), "success_pipeline should not have failed"
    assert oom_result.failed(), "oom_pipeline should have failed"


def test_resource_pool_dependencies():
    """Test that container fails when trying to run operator with unsatisfied dependencies"""
    pool = ResourcePool(
        pool_id=0,
        cpu_pool=10,
        ram_pool=100,
        ticks_per_second=10,
        multi_operator_containers=True
    )

    # Create pipeline with A -> B
    pipeline = Pipeline(pipeline_id="dep_test", priority=Priority.BATCH_PIPELINE)
    op_a = pipeline.new_operator()
    op_b = pipeline.new_operator([op_a])

    op_a.add_segment(Segment(baseline_cpu_seconds=0.1, cpu_scaling="const", memory_gb=5))
    op_b.add_segment(Segment(baseline_cpu_seconds=0.1, cpu_scaling="const", memory_gb=5))

    # Assign B before A completes - assignment succeeds (PENDING -> ASSIGNED)
    # but container will fail when trying to transition B to RUNNING
    assignment_b = Assignment(
        ops=[op_b],
        cpu=2,
        ram=10,
        priority=Priority.BATCH_PIPELINE,
        pool_id=0,
        pipeline_id="dep_test"
    )

    with pytest.raises(AssertionError, match="Dependencies not satisfied"):
        pool.run_one_tick([], [assignment_b])


def test_runtime_status_dependencies():
    """Op b depends on a.  Keep trying to transition b to running.
    Make sure the transition is always rejected, until a is complete."""
    # Create pipeline with A -> B
    pipeline = Pipeline(pipeline_id="test_pipeline", priority=Priority.BATCH_PIPELINE)
    op_a = pipeline.new_operator()
    op_b = pipeline.new_operator([op_a])

    # Initialize runtime status
    status = pipeline.runtime_status()

    op_b.transition(OperatorState.ASSIGNED)
    with pytest.raises(AssertionError):
        op_b.transition(OperatorState.RUNNING)
    
    op_a.transition(OperatorState.ASSIGNED)
    with pytest.raises(AssertionError):
        op_b.transition(OperatorState.RUNNING)

    op_a.transition(OperatorState.RUNNING)
    with pytest.raises(AssertionError):
        op_b.transition(OperatorState.RUNNING)

    op_a.transition(OperatorState.COMPLETED)
    op_b.transition(OperatorState.RUNNING)


def test_runtime_status_state_transitions():
    """Test PipelineRuntimeStatus state tracking through operator lifecycle"""
    # Create simple pipeline with one operator
    pipeline = Pipeline(pipeline_id="state_test", priority=Priority.BATCH_PIPELINE)
    op = pipeline.new_operator()

    status = pipeline.runtime_status()

    # Initially PENDING
    assert status.operator_states[op] == OperatorState.PENDING

    # Transition to ASSIGNED
    op.transition(OperatorState.ASSIGNED)
    assert status.operator_states[op] == OperatorState.ASSIGNED

    # Cannot double assign
    with pytest.raises(AssertionError):
        op.transition(OperatorState.ASSIGNED)
        assert status.operator_states[op] == OperatorState.ASSIGNED

    # Transition to RUNNING
    op.transition(OperatorState.RUNNING)
    assert status.operator_states[op] == OperatorState.RUNNING

    # Transition to COMPLETED
    op.transition(OperatorState.COMPLETED)
    assert status.operator_states[op] == OperatorState.COMPLETED

    # Pipeline should be successful
    assert status.is_pipeline_successful()


def test_memory_allocated_vs_consumed():
    """Test that allocated memory jumps immediately while consumed grows over time.

    Setup:
    - Executor with 2 pools, 100GB each (200GB total)
    - One container allocated 100GB (50% of total)
    - I/O reads 50GB, so memory grows from 0 to 50GB (25% of total)
    """
    ram_per_pool = 100
    ticks_per_second = 10
    executor = Executor(
        num_pools=2,
        cpus_per_pool=10,
        ram_gb_per_pool=ram_per_pool,
        ticks_per_second=ticks_per_second,
    )
    tick_length = 1.0 / ticks_per_second

    pipeline = Pipeline(pipeline_id="mem_test", priority=Priority.BATCH_PIPELINE)
    op = pipeline.new_operator()
    storage_read = 50
    op.add_segment(Segment(
        baseline_cpu_seconds=0,
        memory_gb=None,
        storage_read_gb=storage_read,
    ))

    allocation_ram = ram_per_pool
    assignment = Assignment(
        ops=[op], cpu=1, ram=allocation_ram,
        priority=Priority.BATCH_PIPELINE, pool_id=0, pipeline_id="mem_test",
    )

    completed = False
    for tick in range(1000):
        assignments = [assignment] if tick == 0 else []
        results = executor.run_one_tick([], assignments)

        if results:
            # Container just completed: both should be 0
            assert not results[0].failed()
            expected_allocated = 0
            expected_consumed = 0
            completed = True
        else:
            # During I/O: allocation is fixed, consumption grows
            expected_allocated = allocation_ram
            expected_consumed = (tick + 1) * tick_length * DISK_SCAN_GB_SEC

        assert executor.get_allocated_ram_gb() == expected_allocated, f"tick {tick}"
        assert executor.get_consumed_ram_gb() == expected_consumed, f"tick {tick}"

        if completed:
            break

    assert completed, "Container should have completed"
