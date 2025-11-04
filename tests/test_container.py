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

    # the first 3 containers should run (30s/30 ticks), then we should
    assert ticks_executed == 30, \
        f"Container should stop at OOM at 30 ticks, but ran for {ticks_executed} ticks"
    assert container.error == "OOM", "Container should have OOM error after completion"


def test_container_tick0_oom():
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

    # Container should have 0 ticks since it OOMs immediately
    assert container.num_ticks == 0, f"Expected 0 ticks for immediate OOM, got {container.num_ticks}"
    assert container.error == "OOM", "Container should have OOM error"

    # Container should already be completed (0 ticks to run)
    assert container.is_completed(), "Container with 0 ticks should be immediately completed"

    # Calling tick() shouldn't break the is_completed() check
    container.tick()
    assert container.is_completed(), "Container should still be completed after tick()"
