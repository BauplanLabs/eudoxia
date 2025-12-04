import pytest
from eudoxia.executor.container import Container
from eudoxia.workload.pipeline import Segment, Operator
from eudoxia.utils import Priority, DISK_SCAN_GB_SEC


def test_container_oom():
    """Test that OOM in first operator prevents subsequent operators from being calculated"""

    # Segments with mem usage 10 GB, 20 GB, 30 GB, etc
    ops = []
    for i in range(10):
        op = Operator()
        op.add_segment(Segment(
            baseline_cpu_seconds = 10,
            memory_gb =10*(i+1),
        ))
        ops.append(op)

    # Create container with limited RAM
    container = Container(
        ram=35,
        cpu=10,
        ops=ops,
        prty=Priority.BATCH_PIPELINE,
        pool_id=0,
        ticks_per_second=1
    )

    # Run the container to completion
    ticks_executed = 0
    while not container.is_completed():
        container.tick()
        ticks_executed += 1

        assert ticks_executed <= 1000, "Container should complete within 1000 ticks"

    # the first 3 operators should run (30 ticks), then the 4th operator (40GB)
    # triggers OOM on tick 31 when we try to execute it
    assert ticks_executed == 31, \
        f"Container should stop at OOM at 31 ticks, but ran for {ticks_executed} ticks"
    assert container.error == "OOM", "Container should have OOM error after completion"


def test_container_immediate_oom():
    """Test that immediate OOM (when segment needs more memory than container has) completes properly"""

    # Create a segment that needs 200GB of memory
    op = Operator()
    op.add_segment(Segment(
        baseline_cpu_seconds=1.0,
        cpu_scaling="const",
        memory_gb=200,  # Needs 200GB
        storage_read_gb=0
    ))

    # Create container with only 50GB RAM - immediate OOM
    container = Container(
        ram=50,  # Only 50GB available
        cpu=10,
        ops=[op],
        prty=Priority.BATCH_PIPELINE,
        pool_id=0,
        ticks_per_second=10
    )

    # Container is not yet completed - OOM detected on first tick
    assert not container.is_completed(), "Container should not be completed before first tick"

    # First tick should detect OOM
    container.tick()
    assert container.ticks_elapsed() == 1, f"Expected 1 tick before OOM, got {container.ticks_elapsed()}"
    assert container.error == "OOM", "Container should have OOM error"
    assert container.is_completed(), "Container should be completed after OOM"

    # Calling tick() again shouldn't break anything
    container.tick()
    assert container.is_completed(), "Container should still be completed after extra tick()"


def test_container_suspension_ticks():
    """Test that suspension takes the correct number of ticks to write memory to disk"""

    op = Operator()
    op.add_segment(Segment(
        baseline_cpu_seconds=1.0,
        cpu_scaling="const",
        memory_gb=10,
    ))

    # 100 ticks per second, so tick_length = 0.01 seconds
    container = Container(
        ram=100,  # 100GB RAM
        cpu=10,
        ops=[op],
        prty=Priority.BATCH_PIPELINE,
        pool_id=0,
        ticks_per_second=100
    )

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
    # Op1: fixed memory
    op1 = Operator()
    op1.add_segment(Segment(
        baseline_cpu_seconds=1.0,
        cpu_scaling="const",
        memory_gb=10,
        storage_read_gb=0,
    ))

    # Op2: I/O-based memory (memory_gb=None means memory grows with I/O)
    op2 = Operator()
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

    container = Container(
        ram=100,
        cpu=1,  # 1 CPU so baseline_cpu_seconds = actual seconds
        ops=[op1, op2],
        prty=Priority.BATCH_PIPELINE,
        pool_id=0,
        ticks_per_second=10,
    )

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
