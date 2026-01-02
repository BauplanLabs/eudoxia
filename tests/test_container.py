import pytest
from eudoxia.executor.container import Container
from eudoxia.executor.resource_pool import ResourcePool
from eudoxia.executor.assignment import Assignment
from eudoxia.workload.pipeline import Segment, Operator, Pipeline
from eudoxia.utils import Priority, DISK_SCAN_GB_SEC


class MockPool:
    """Mock pool for testing containers without OOM killing.
    Use for tests where memory stays within limits.
    """
    def __init__(self, pool_id=0):
        self.pool_id = pool_id
        self.consumed_ram_gb = 0.0


def test_container_oom():
    """Test that OOM in first operator prevents subsequent operators from being calculated"""

    # Segments with mem usage 10 GB, 20 GB, 30 GB, etc
    pipeline = Pipeline(pipeline_id="oom_test", priority=Priority.BATCH_PIPELINE)
    ops = []
    prev_op = None
    for i in range(10):
        op = pipeline.new_operator([prev_op] if prev_op else None)
        op.add_segment(Segment(
            baseline_cpu_seconds = 10,
            memory_gb =10*(i+1),
        ))
        ops.append(op)
        prev_op = op

    # Create pool and assignment with limited RAM
    pool = ResourcePool(pool_id=0, cpu_pool=100, ram_pool=100, ticks_per_second=1)
    assignment = Assignment(
        ops=ops, cpu=10, ram=35,
        priority=Priority.BATCH_PIPELINE, pool_id=0, pipeline_id="oom_test"
    )

    # Start container via pool
    pool.run_one_tick([], [assignment])
    container = pool.active_containers[0]

    # Run until completion
    ticks_executed = 1  # First tick already done
    while not container.is_completed():
        pool.run_one_tick([], [])
        ticks_executed += 1
        assert ticks_executed <= 1000, "Container should complete within 1000 ticks"

    # the first 3 operators should run (30 ticks), then the 4th operator (40GB)
    # triggers OOM on tick 31 when we try to execute it
    assert ticks_executed == 31, \
        f"Container should stop at OOM at 31 ticks, but ran for {ticks_executed} ticks"
    assert container.error == "OOM", "Container should have OOM error after completion"


def test_container_oom_transitions_remaining_ops_to_failed():
    """Test that when a container OOMs, all operators (running and assigned) transition to FAILED."""
    from eudoxia.workload import OperatorState

    pipeline = Pipeline(pipeline_id="oom_fail_test", priority=Priority.BATCH_PIPELINE)
    op_a = pipeline.new_operator()
    op_b = pipeline.new_operator([op_a])
    op_c = pipeline.new_operator([op_b])

    # op_b will OOM (needs 100GB); op_a should be completed by then, and op_c should be considered failed
    op_a.add_segment(Segment(baseline_cpu_seconds=1.0, memory_gb=10))
    op_b.add_segment(Segment(baseline_cpu_seconds=1.0, memory_gb=100))
    op_c.add_segment(Segment(baseline_cpu_seconds=1.0, memory_gb=10))

    pool = ResourcePool(pool_id=0, cpu_pool=100, ram_pool=100, ticks_per_second=10)
    assignment = Assignment(
        ops=[op_a, op_b, op_c], cpu=10, ram=50,  # Not enough for op_b
        priority=Priority.BATCH_PIPELINE, pool_id=0, pipeline_id="oom_fail_test"
    )

    # Start container and run until OOM
    pool.run_one_tick([], [assignment])
    container = pool.active_containers[0]
    while not container.is_completed():
        pool.run_one_tick([], [])

    assert container.error == "OOM"

    # All operators should be FAILED (not stuck in ASSIGNED)
    status = pipeline.runtime_status()
    assert status.operator_states[op_a] == OperatorState.COMPLETED
    assert status.operator_states[op_b] == OperatorState.FAILED
    assert status.operator_states[op_c] == OperatorState.FAILED


def test_container_immediate_oom():
    """Test that immediate OOM (when segment needs more memory than container has) completes properly"""

    pipeline = Pipeline(pipeline_id="immediate_oom_test", priority=Priority.BATCH_PIPELINE)
    # Create a segment that needs 200GB of memory
    op = pipeline.new_operator()
    op.add_segment(Segment(
        baseline_cpu_seconds=1.0,
        cpu_scaling="const",
        memory_gb=200,  # Needs 200GB
        storage_read_gb=0
    ))

    # Create pool and assignment with only 50GB RAM - immediate OOM
    pool = ResourcePool(pool_id=0, cpu_pool=100, ram_pool=500, ticks_per_second=10)
    assignment = Assignment(
        ops=[op], cpu=10, ram=50,  # Only 50GB available
        priority=Priority.BATCH_PIPELINE, pool_id=0, pipeline_id="immediate_oom_test"
    )

    # First tick creates container and runs OOM killer
    results = pool.run_one_tick([], [assignment])

    # Container should be killed immediately
    assert len(results) == 1, "Should have one result"
    assert results[0].error == "OOM", "Container should have OOM error"


def test_container_suspension_ticks():
    """Test that suspension takes the correct number of ticks to write memory to disk"""

    pipeline = Pipeline(pipeline_id="suspend_test", priority=Priority.BATCH_PIPELINE)
    op = pipeline.new_operator()
    op.add_segment(Segment(
        baseline_cpu_seconds=1.0,
        cpu_scaling="const",
        memory_gb=10,
    ))

    # 100 ticks per second, so tick_length = 0.01 seconds
    assignment = Assignment(
        ops=[op], cpu=10, ram=100,  # 100GB RAM
        priority=Priority.BATCH_PIPELINE, pool_id=0, pipeline_id="suspend_test"
    )
    container = Container(assignment=assignment, pool=MockPool(), ticks_per_second=100)

    # Trigger suspension
    container.suspend_container()

    # Suspension should take: ram / DISK_SCAN_GB_SEC seconds = 100 / 20 = 5 seconds
    # At 100 ticks/second, that's 500 ticks
    expected_suspend_ticks = int((100 / DISK_SCAN_GB_SEC) / container.tick_length_secs)
    assert container.suspend_ticks == expected_suspend_ticks, \
        f"Expected {expected_suspend_ticks} suspend ticks, got {container.suspend_ticks}"
    assert container.suspend_ticks == 500, \
        f"Expected 500 suspend ticks, got {container.suspend_ticks}"

    # Tick through suspension
    suspend_ticks_counted = 0
    while not container.is_suspended():
        container.suspend_container_tick()
        suspend_ticks_counted += 1

    assert suspend_ticks_counted == 500, \
        f"Expected 500 ticks to suspend, counted {suspend_ticks_counted}"


def test_container_memory_over_time():
    """Test that memory usage is tracked correctly over time.

    Op1: single segment with fixed memory (10GB), 1 second CPU
    Op2: two segments with I/O-based memory growth
        - seg1: 2 seconds I/O (memory grows 0->40GB), 1 second CPU (stays at 40GB)
        - seg2: 1 second I/O (memory grows 0->20GB), 0.5 second CPU (stays at 20GB)

    At 10 ticks/second:
    - Op1 seg1: 10 CPU ticks at 10GB fixed
    - Op2 seg1: 20 I/O ticks (growing 2->40GB), then 10 CPU ticks at 40GB
    - Op2 seg2: 10 I/O ticks (growing 2->20GB), then 5 CPU ticks at 20GB
    """
    pipeline = Pipeline(pipeline_id="memory_test", priority=Priority.BATCH_PIPELINE)
    # Op1: fixed memory
    op1 = pipeline.new_operator()
    op1.add_segment(Segment(
        baseline_cpu_seconds=1.0,
        cpu_scaling="const",
        memory_gb=10,
        storage_read_gb=0,
    ))

    # Op2: I/O-based memory (memory_gb=None means memory grows with I/O)
    op2 = pipeline.new_operator([op1])
    op2.add_segment(Segment(
        baseline_cpu_seconds=1.0,
        cpu_scaling="const",
        memory_gb=None,
        storage_read_gb=40,  # 40GB read at 20GB/sec = 2 sec I/O, peak memory = 40GB
    ))
    op2.add_segment(Segment(
        baseline_cpu_seconds=0.5,
        cpu_scaling="const",
        memory_gb=None,
        storage_read_gb=20,  # 20GB read at 20GB/sec = 1 sec I/O, peak memory = 20GB
    ))

    assignment = Assignment(
        ops=[op1, op2], cpu=1, ram=100,  # 1 CPU so baseline_cpu_seconds = actual seconds
        priority=Priority.BATCH_PIPELINE, pool_id=0, pipeline_id="memory_test"
    )
    container = Container(assignment=assignment, pool=MockPool(), ticks_per_second=10)

    tick_length = 0.1  # 10 ticks/sec
    expected_memories = []

    # Op1 seg1: 10 CPU ticks at 10GB (no I/O since storage_read_gb=0)
    for _ in range(10):
        expected_memories.append(10.0)

    # Op2 seg1: 20 I/O ticks with memory growing linearly
    # Memory grows at DISK_SCAN_GB_SEC (20 GB/sec)
    # After tick i, memory = (i+1) * tick_length * 20 = (i+1) * 2
    for i in range(20):
        expected_memories.append((i + 1) * tick_length * DISK_SCAN_GB_SEC)

    # Op2 seg1: 10 CPU ticks at peak memory (40GB)
    for _ in range(10):
        expected_memories.append(40.0)

    # Op2 seg2: 10 I/O ticks with memory growing linearly
    for i in range(10):
        expected_memories.append((i + 1) * tick_length * DISK_SCAN_GB_SEC)

    # Op2 seg2: 5 CPU ticks at peak memory (20GB)
    # Last tick will show 0.0 since container completes
    for i in range(5):
        if i < 4:
            expected_memories.append(20.0)
        else:
            expected_memories.append(0.0)  # Container completes, memory cleared

    # Run through all ticks and verify memory
    tick = 0
    while not container.is_completed():
        container.tick()
        actual_memory = container.get_current_memory_usage()
        expected_memory = expected_memories[tick]
        assert actual_memory == expected_memory, \
            f"Tick {tick}: expected memory {expected_memory}GB, got {actual_memory}GB"
        tick += 1

    assert tick == len(expected_memories), \
        f"Expected {len(expected_memories)} ticks, got {tick}"
    assert tick == 55, f"Expected 55 total ticks, got {tick}"
