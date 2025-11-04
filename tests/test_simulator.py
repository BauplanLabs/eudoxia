import pytest
from typing import Dict, List
from eudoxia.simulator import run_simulator, get_param_defaults
from eudoxia.workload import Workload, Pipeline
from eudoxia.utils import Priority

class MockWorkload(Workload):
    """
    Mock workload that returns pre-defined pipelines at specific times.
    Useful for testing schedulers with controlled workloads.
    """

    def __init__(self, workload_schedule: Dict[float, List[Pipeline]], ticks_per_second: int):
        """
        Args:
            workload_schedule: Dict mapping arrival time in seconds to list of pipelines to return
            ticks_per_second: Simulation ticks per second (for converting seconds to ticks)
        """
        # Convert seconds-based schedule to tick-based schedule
        self.workload_schedule = {}
        for arrival_seconds, pipelines in workload_schedule.items():
            tick = int(arrival_seconds * ticks_per_second)
            self.workload_schedule[tick] = pipelines
        self.current_tick = 0

    def run_one_tick(self) -> List[Pipeline]:
        """Return pipelines scheduled for the current tick"""
        pipelines = self.workload_schedule.get(self.current_tick, [])
        self.current_tick += 1
        return pipelines


@pytest.mark.parametrize("scheduler_algo", ["naive", "priority", "priority-pool"])
def test_run_simulator_basic(scheduler_algo):
    """Test each scheduler with a controlled workload"""
    from eudoxia.workload import Operator, Segment

    # configure scheduler
    params = get_param_defaults()
    params['scheduler_algo'] = scheduler_algo
    if scheduler_algo == "priority-pool":
        params['num_pools'] = 2  # priority-pool scheduler requires 2 pools

    # simple workload with 3 jobs
    pipelines = []
    for i in range(3):
        pipeline = Pipeline(f"test{i+1}", Priority.BATCH_PIPELINE)
        op = Operator()
        # CPU-only segment (no I/O): storage_read_gb=0 means no I/O time
        seg = Segment(baseline_cpu_seconds=3, cpu_scaling="const", storage_read_gb=0)
        op.add_segment(seg)
        pipeline.add_operator(op)
        pipelines.append(pipeline)

    workload = MockWorkload({
        1.0: [pipelines[0]],
        2.0: [pipelines[1]],
        3.0: [pipelines[2]],
    }, params['ticks_per_second'])

    params['duration'] = 5.5

    if scheduler_algo == "naive":
        # they are run one at a time, so at 5.5 seconds we will only have
        # completed one of the 3 second jobs
        expected_completions = 1
    else:
        # all jobs can run concurrently, and take 3 seconds.
        # At time 5.5, the first two jobs will be done, but not the last one
        expected_completions = 2

    # run simulator and check results
    stats = run_simulator(params, workload=workload)
    assert stats.pipelines_created == 3, f"{scheduler_algo}: Expected 3 created pipelines, got {stats.pipelines_created}"
    assert stats.pipelines_completed == expected_completions, f"{scheduler_algo}: Expected {expected_completions} completed pipelines, got {stats.pipelines_completed}"


@pytest.mark.parametrize("ticks_per_second", [100_000, 10_000, 1_000, 100])  # 10us, 100us, 1ms, 10ms
def test_tick_length(ticks_per_second):
    """Test that tick length affects execution timing correctly"""
    from eudoxia.workload import Operator, Segment

    # Configure for precise timing test
    params = get_param_defaults()
    params.update({
        'scheduler_algo': 'naive',
        'num_pools': 1,
        'cpu_pool': 1,  # Only 1 CPU so jobs run sequentially
        'duration': 4.9,  # 4.9 seconds - should allow only 4 jobs to complete
        'ticks_per_second': ticks_per_second,
    })

    # Create 10 pipelines that arrive immediately
    pipelines = []
    for i in range(10):
        pipeline = Pipeline(f"cpu_test{i+1}", Priority.BATCH_PIPELINE)
        op = Operator()
        # CPU-only segment: 0 IO, 1 second CPU time
        seg = Segment(baseline_cpu_seconds=1.0, cpu_scaling="const", storage_read_gb=0)
        op.add_segment(seg)
        pipeline.add_operator(op)
        pipelines.append(pipeline)

    # All pipelines arrive at 0 seconds
    workload_schedule = {0.0: pipelines}
    mock_workload = MockWorkload(workload_schedule, ticks_per_second)

    # Run simulation
    stats = run_simulator(params, workload=mock_workload)
    
    # Verify: 10 jobs created, but only 4 complete in 4.9 seconds
    # (each job takes 1 second, and with 1 CPU they run sequentially)
    assert stats.pipelines_created == 10, f"Expected 10 pipelines created, got {stats.pipelines_created}"
    assert stats.pipelines_completed == 4, f"Expected 4 pipelines completed, got {stats.pipelines_completed}"


@pytest.mark.parametrize("scheduler_algo", ["priority", "priority-pool"])
def test_oom_retry(scheduler_algo):
    """Test that schedulers handle OOM errors and retry with more resources"""
    from eudoxia.workload import Operator, Segment

    # Configure with limited resources
    params = get_param_defaults()
    params['scheduler_algo'] = scheduler_algo
    params['ram_pool'] = 100  # 100 GB RAM total
    params['cpu_pool'] = 8
    params['duration'] = 10.0
    if scheduler_algo == "priority-pool":
        params['num_pools'] = 2

    # Create a single pipeline with high memory requirement
    # Reading 20GB of data means needing 20GB RAM (memory_gb defaults to storage_read_gb)
    # Priority schedulers will retry with doubled RAM until it fits (and is < 50% of pool)
    # Note: priority-pool splits resources between 2 pools, so each pool has 50GB
    pipeline = Pipeline("oom_test", Priority.BATCH_PIPELINE)
    op = Operator()
    seg = Segment(baseline_cpu_seconds=1.0, cpu_scaling="const", storage_read_gb=20)
    op.add_segment(seg)
    pipeline.add_operator(op)

    workload = MockWorkload({0.0: [pipeline]}, params['ticks_per_second'])

    # Run simulation
    stats = run_simulator(params, workload=workload)

    # Verify pipeline was created and eventually completed
    assert stats.pipelines_created == 1, f"{scheduler_algo}: Expected 1 pipeline created, got {stats.pipelines_created}"
    assert stats.pipelines_completed == 1, f"{scheduler_algo}: Expected 1 pipeline completed after OOM retry, got {stats.pipelines_completed}"

    # Verify retries happened: failures = assignments - 1 (last assignment succeeds)
    assert stats.assignments > 1, f"{scheduler_algo}: Expected multiple assignments (retries), got {stats.assignments}"
    assert stats.failures == stats.assignments - 1, f"{scheduler_algo}: Expected failures={stats.assignments-1}, got {stats.failures}"
    assert 'OOM' in stats.failure_error_counts, f"{scheduler_algo}: Expected OOM errors in failure counts"
    assert stats.failure_error_counts['OOM'] == stats.failures, f"{scheduler_algo}: All failures should be OOM"
