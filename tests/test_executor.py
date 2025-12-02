import pytest
from eudoxia.executor.resource_pool import ResourcePool
from eudoxia.executor.assignment import Assignment, Suspend
from eudoxia.workload.pipeline import Segment, Operator, Pipeline
from eudoxia.workload import OperatorState
from eudoxia.utils import Priority


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
    op_success = Operator()
    op_success.add_segment(Segment(
        baseline_cpu_seconds=0.1,  # 0.1 seconds
        cpu_scaling="const",
        memory_gb=10,  # Reasonable memory usage
        storage_read_gb=0
    ))
    pipeline_success.add_operator(op_success)

    # Create pipeline and operator for OOM container
    # Use storage_read_gb with no fixed memory_gb to get linear growth OOM
    pipeline_oom = Pipeline(pipeline_id="oom_pipeline", priority=Priority.BATCH_PIPELINE)
    op_oom = Operator()
    op_oom.add_segment(Segment(
        baseline_cpu_seconds=0.1,  # 0.1 seconds
        cpu_scaling="const",
        storage_read_gb=200  # Linear memory growth - will OOM when it hits 50GB limit
        # memory_gb not set, so memory grows linearly as storage is read
    ))
    pipeline_oom.add_operator(op_oom)

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
    """Test that ResourcePool rejects operators assigned out of order"""
    pool = ResourcePool(
        pool_id=0,
        cpu_pool=10,
        ram_pool=100,
        ticks_per_second=10,
        multi_operator_containers=True
    )

    # Create pipeline with A -> B
    pipeline = Pipeline(pipeline_id="dep_test", priority=Priority.BATCH_PIPELINE)
    op_a = Operator()
    op_b = Operator()

    op_a.add_segment(Segment(baseline_cpu_seconds=0.1, cpu_scaling="const", memory_gb=5))
    op_b.add_segment(Segment(baseline_cpu_seconds=0.1, cpu_scaling="const", memory_gb=5))

    pipeline.add_operator(op_a)
    pipeline.add_operator(op_b, [op_a])

    # Try to assign B before A completes (should fail)
    assignment_b = Assignment(
        ops=[op_b],
        cpu=2,
        ram=10,
        priority=Priority.BATCH_PIPELINE,
        pool_id=0,
        pipeline_id="dep_test"
    )

    results = pool.run_one_tick([], [assignment_b])
    assert len(results) == 1
    assert results[0].failed()
    assert results[0].error == "dependency"


def test_runtime_status_dependencies():
    """Test PipelineRuntimeStatus correctly validates operator scheduling"""
    # Create pipeline with A -> B
    pipeline = Pipeline(pipeline_id="test_pipeline", priority=Priority.BATCH_PIPELINE)
    op_a = Operator()
    op_b = Operator()

    pipeline.add_operator(op_a)
    pipeline.add_operator(op_b, [op_a])

    # Initialize runtime status
    status = pipeline.runtime_status()

    # Try to schedule B before A completes (should fail)
    can_schedule, error = status.can_schedule(op_b)
    assert not can_schedule
    assert "Dependencies not satisfied" in error

    # A should be schedulable
    can_schedule, error = status.can_schedule(op_a)
    assert can_schedule
    assert error is None


def test_runtime_status_state_transitions():
    """Test PipelineRuntimeStatus state tracking through operator lifecycle"""
    # Create simple pipeline with one operator
    pipeline = Pipeline(pipeline_id="state_test", priority=Priority.BATCH_PIPELINE)
    op = Operator()
    pipeline.add_operator(op)

    status = pipeline.runtime_status()

    # Initially PENDING
    assert status.operator_states[op] == OperatorState.PENDING

    # Transition to RUNNING
    op.transition(OperatorState.RUNNING)
    assert status.operator_states[op] == OperatorState.RUNNING

    # Transition to COMPLETED
    op.transition(OperatorState.COMPLETED)
    assert status.operator_states[op] == OperatorState.COMPLETED

    # Pipeline should be successful
    assert status.is_pipeline_successful()
