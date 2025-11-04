import pytest
from eudoxia.executor.resource_pool import ResourcePool
from eudoxia.executor.assignment import Assignment, Suspend
from eudoxia.workload.pipeline import Segment, Operator, Pipeline
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
        ticks_per_second=10
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
