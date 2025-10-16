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
