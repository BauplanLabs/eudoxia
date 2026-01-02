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


def test_memory_overcommit_kills_highest_scorer():
    """Test that pool-level OOM kills containers with highest score.

    Setup:
    - Pool with 100GB capacity, overcommit enabled
    - Two containers, each allocated 80GB (160% total allocation)
    - Container A: I/O grows to 60GB (75% of allocation, score = 60 * 0.75 = 45)
    - Container B: I/O grows to 50GB (62.5% of allocation, score = 50 * 0.625 = 31.25)
    - When total consumption (110GB) exceeds capacity (100GB), A should be killed

    The container with higher score (A) should be killed first.
    """
    ram_pool = 100
    ticks_per_second = 10
    pool = ResourcePool(
        pool_id=0,
        cpu_pool=100,
        ram_pool=ram_pool,
        ticks_per_second=ticks_per_second,
        multi_operator_containers=True,
        allow_memory_overcommit=True,
    )
    tick_length = 1.0 / ticks_per_second

    # Container A: allocate 80GB, will consume up to 60GB
    pipeline_a = Pipeline(pipeline_id="a", priority=Priority.BATCH_PIPELINE)
    op_a = pipeline_a.new_operator()
    op_a.add_segment(Segment(
        baseline_cpu_seconds=10,  # Long CPU phase so it doesn't complete
        memory_gb=None,
        storage_read_gb=60,  # Will grow to 60GB during I/O
    ))
    assignment_a = Assignment(
        ops=[op_a], cpu=1, ram=80,
        priority=Priority.BATCH_PIPELINE, pool_id=0, pipeline_id="a",
    )

    # Container B: allocate 80GB, will consume up to 50GB
    pipeline_b = Pipeline(pipeline_id="b", priority=Priority.BATCH_PIPELINE)
    op_b = pipeline_b.new_operator()
    op_b.add_segment(Segment(
        baseline_cpu_seconds=10,
        memory_gb=None,
        storage_read_gb=50,  # Will grow to 50GB during I/O
    ))
    assignment_b = Assignment(
        ops=[op_b], cpu=1, ram=80,
        priority=Priority.BATCH_PIPELINE, pool_id=0, pipeline_id="b",
    )

    # Start both containers
    pool.run_one_tick([], [assignment_a, assignment_b])

    # Tick until consumption exceeds capacity
    # Memory grows at DISK_SCAN_GB_SEC = 20 GB/sec
    # After tick t: consumption_a = t * 0.1 * 20 = 2t, consumption_b = 2t
    # Total = 4t, exceeds 100GB when t > 25
    # But A caps at 60GB after 30 ticks, B caps at 50GB after 25 ticks

    killed_pipeline = None
    for tick in range(100):
        results = pool.run_one_tick([], [])
        for r in results:
            if r.failed():
                # Get pipeline ID from the failed container's ops
                killed_pipeline = r.ops[0].pipeline.pipeline_id
                break
        if killed_pipeline:
            break

    assert killed_pipeline is not None, "A container should have been killed"

    # Verify the killed container is A (higher score)
    # At time of kill, A has higher consumption and similar percentage
    # Score_A = consumption_A * (consumption_A / 80)
    # Score_B = consumption_B * (consumption_B / 80)
    # Since consumption_A > consumption_B at time of OOM, A should be killed
    assert killed_pipeline == "a", f"Container A should be killed first, got pipeline {killed_pipeline}"

    # Pool consumption should now be below capacity
    assert pool.get_consumed_ram_gb() <= ram_pool
