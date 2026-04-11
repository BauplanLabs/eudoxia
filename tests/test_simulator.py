import pytest
import math
from typing import Dict, List
from eudoxia.simulator import run_simulator, get_param_defaults, SimulatorStats, PipelineStats
from eudoxia.workload import Workload, Pipeline, Segment
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
    params['ticks_per_second'] = 1000
    if scheduler_algo == "priority-pool":
        params['num_pools'] = 2  # priority-pool scheduler requires 2 pools
    elif scheduler_algo == "naive":
        params['num_pools'] = 1  # naive runs one job at a time per pool

    # simple workload with 3 jobs
    pipelines = []
    for i in range(3):
        pipeline = Pipeline(f"test{i+1}", Priority.BATCH_PIPELINE)
        op = pipeline.new_operator()
        # CPU-only segment (no I/O): storage_read_gb=0 means no I/O time
        seg = Segment(baseline_cpu_seconds=3, cpu_scaling="const", storage_read_gb=0)
        op.add_segment(seg)
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
    assert stats.containers_completed == expected_completions, f"{scheduler_algo}: Expected {expected_completions} completed containers, got {stats.containers_completed}"


@pytest.mark.parametrize("ticks_per_second", [100_000, 10_000, 1_000, 100])  # 10us, 100us, 1ms, 10ms
def test_tick_length(ticks_per_second):
    """Test that tick length affects execution timing correctly"""
    from eudoxia.workload import Operator, Segment

    # Configure for precise timing test
    params = get_param_defaults()
    params.update({
        'scheduler_algo': 'naive',
        'num_pools': 1,
        'cpus_per_pool': 1,  # Only 1 CPU so jobs run sequentially
        'ram_gb_per_pool': 64,
        'duration': 4.9,  # 4.9 seconds - should allow only 4 jobs to complete
        'ticks_per_second': ticks_per_second,
    })

    # Create 10 pipelines that arrive immediately
    pipelines = []
    for i in range(10):
        pipeline = Pipeline(f"cpu_test{i+1}", Priority.BATCH_PIPELINE)
        op = pipeline.new_operator()
        # CPU-only segment: 0 IO, 1 second CPU time
        seg = Segment(baseline_cpu_seconds=1.0, cpu_scaling="const", storage_read_gb=0)
        op.add_segment(seg)
        pipelines.append(pipeline)

    # All pipelines arrive at 0 seconds
    workload_schedule = {0.0: pipelines}
    mock_workload = MockWorkload(workload_schedule, ticks_per_second)

    # Run simulation
    stats = run_simulator(params, workload=mock_workload)
    
    # Verify: 10 jobs created, but only 4 complete in 4.9 seconds
    # (each job takes 1 second, and with 1 CPU they run sequentially)
    assert stats.pipelines_created == 10, f"Expected 10 pipelines created, got {stats.pipelines_created}"
    assert stats.containers_completed == 4, f"Expected 4 containers completed, got {stats.containers_completed}"


@pytest.mark.parametrize("scheduler_algo", ["priority", "priority-pool"])
def test_oom_retry(scheduler_algo):
    """Test that schedulers handle OOM errors and retry with more resources"""
    from eudoxia.workload import Operator, Segment

    # Configure with limited resources
    params = get_param_defaults()
    params['scheduler_algo'] = scheduler_algo
    params['ram_gb_per_pool'] = 100
    params['cpus_per_pool'] = 8
    params['duration'] = 10.0
    params['ticks_per_second'] = 1000
    if scheduler_algo == "priority-pool":
        params['num_pools'] = 2

    # Create a single pipeline with high memory requirement
    # Reading 20GB of data means needing 20GB RAM (memory_gb defaults to storage_read_gb)
    # Priority schedulers will retry with doubled RAM until it fits (and is < 50% of pool)
    # Note: priority-pool splits resources between 2 pools, so each pool has 50GB
    pipeline = Pipeline("oom_test", Priority.BATCH_PIPELINE)
    op = pipeline.new_operator()
    seg = Segment(baseline_cpu_seconds=1.0, cpu_scaling="const", storage_read_gb=20)
    op.add_segment(seg)

    workload = MockWorkload({0.0: [pipeline]}, params['ticks_per_second'])

    # Run simulation
    stats = run_simulator(params, workload=workload)

    # Verify pipeline was created and eventually completed
    assert stats.pipelines_created == 1, f"{scheduler_algo}: Expected 1 pipeline created, got {stats.pipelines_created}"
    assert stats.containers_completed == 1, f"{scheduler_algo}: Expected 1 container completed after OOM retry, got {stats.containers_completed}"

    # Verify retries happened: failures = assignments - 1 (last assignment succeeds)
    assert stats.assignments > 1, f"{scheduler_algo}: Expected multiple assignments (retries), got {stats.assignments}"
    assert stats.failures == stats.assignments - 1, f"{scheduler_algo}: Expected failures={stats.assignments-1}, got {stats.failures}"
    assert 'OOM' in stats.failure_error_counts, f"{scheduler_algo}: Expected OOM errors in failure counts"
    assert stats.failure_error_counts['OOM'] == stats.failures, f"{scheduler_algo}: All failures should be OOM"


def test_adjusted_latency():
    """Test SimulatorStats.adjusted_latency with progressively complex scenarios."""

    ignored_sim_fields = {
        "pipelines_created": None, "containers_completed": None, "throughput": None, "p99_latency": None,
        "assignments": None, "suspensions": None, "failures": 0, "failure_error_counts": {},
        "mean_memory_allocated_percent": None, "mean_memory_consumed_percent": None,
    }
    ignored_pipe_fields = {
        "timeout_count": 0,
        "p99_latency_seconds": None,
    }

    # Scenario 1: Equal weights, one completion each, 100% completion rate
    stats = SimulatorStats(
        pipelines_all=PipelineStats(
            arrival_count=3, completion_count=3,
            mean_latency_seconds=2.0, **ignored_pipe_fields),

        pipelines_query=PipelineStats(
            arrival_count=1, completion_count=1,
            mean_latency_seconds=1.0, **ignored_pipe_fields),
        pipelines_interactive=PipelineStats(
            arrival_count=1, completion_count=1,
            mean_latency_seconds=2.0, **ignored_pipe_fields),
        pipelines_batch=PipelineStats(
            arrival_count=1, completion_count=1,
            mean_latency_seconds=3.0, **ignored_pipe_fields),
        **ignored_sim_fields,
    )
    weights = {Priority.QUERY: 1, Priority.INTERACTIVE: 1, Priority.BATCH_PIPELINE: 1}
    result = stats.adjusted_latency(weights=weights, divide_by_completion_rate=False)
    assert result == pytest.approx(2.0)

    # Scenario 2: Same stats, different waiting
    weights = {Priority.QUERY: 2, Priority.INTERACTIVE: 1, Priority.BATCH_PIPELINE: 0}
    result = stats.adjusted_latency(weights=weights, divide_by_completion_rate=False)
    assert result == pytest.approx((1+1+2) / 3)

    # Scenario 3: without divide_by_completion_rate, unfinished pipelines don't impact latency
    stats = SimulatorStats(
        pipelines_all=PipelineStats(
            arrival_count=6, completion_count=3,
            mean_latency_seconds=2.0, **ignored_pipe_fields),

        pipelines_query=PipelineStats(
            arrival_count=1, completion_count=1,
            mean_latency_seconds=1.0, **ignored_pipe_fields),
        pipelines_interactive=PipelineStats(
            arrival_count=2, completion_count=1,
            mean_latency_seconds=2.0, **ignored_pipe_fields),
        pipelines_batch=PipelineStats(
            arrival_count=3, completion_count=1,
            mean_latency_seconds=3.0, **ignored_pipe_fields),
        **ignored_sim_fields,
    )
    weights = {Priority.QUERY: 1, Priority.INTERACTIVE: 1, Priority.BATCH_PIPELINE: 1}
    result = stats.adjusted_latency(weights=weights, divide_by_completion_rate=False)
    assert result == pytest.approx(2.0)

    # Scenario 4: with divide_by_completion_rate, latency adjusts higher.
    # here, half finish, so we 2x latency
    weights = {Priority.QUERY: 1, Priority.INTERACTIVE: 1, Priority.BATCH_PIPELINE: 1}
    result = stats.adjusted_latency(weights=weights, divide_by_completion_rate=True)
    assert result == pytest.approx(4.0)

    # Scenario 5: no completions, inf latency
    ps = PipelineStats(arrival_count=1, completion_count=0,
                       mean_latency_seconds=None, **ignored_pipe_fields)
    stats = SimulatorStats(
        pipelines_all=ps,
        pipelines_query=ps,
        pipelines_interactive=ps,
        pipelines_batch=ps,
        **ignored_sim_fields,
    )
    weights = {Priority.QUERY: 1, Priority.INTERACTIVE: 1, Priority.BATCH_PIPELINE: 1}
    result = stats.adjusted_latency(weights=weights, divide_by_completion_rate=True)
    assert stats.adjusted_latency() == float('inf')

    # Scenario 6: penalty with all completed — penalty doesn't change anything.
    # 3 jobs, all complete, mean latency 2s each, equal weights.
    # result = (2+2+2)/3 = 2.0
    stats = SimulatorStats(
        pipelines_all=PipelineStats(
            arrival_count=3, completion_count=3,
            mean_latency_seconds=2.0, **ignored_pipe_fields),
        pipelines_query=PipelineStats(
            arrival_count=1, completion_count=1,
            mean_latency_seconds=2.0, **ignored_pipe_fields),
        pipelines_interactive=PipelineStats(
            arrival_count=1, completion_count=1,
            mean_latency_seconds=2.0, **ignored_pipe_fields),
        pipelines_batch=PipelineStats(
            arrival_count=1, completion_count=1,
            mean_latency_seconds=2.0, **ignored_pipe_fields),
        **ignored_sim_fields,
    )
    weights = {Priority.QUERY: 1, Priority.INTERACTIVE: 1, Priority.BATCH_PIPELINE: 1}
    result = stats.adjusted_latency(weights=weights, divide_by_completion_rate=False,
                                    unfinished_penalty_seconds=100.0)
    assert result == pytest.approx(2.0)

    # Scenario 7: 2 arrived per category, 1 completed each at 2s, penalty=10s.
    # Equal weights: 3 completed at 2s + 3 unfinished at 10s = (6+30)/6 = 6.0
    stats = SimulatorStats(
        pipelines_all=PipelineStats(
            arrival_count=6, completion_count=3,
            mean_latency_seconds=2.0, **ignored_pipe_fields),
        pipelines_query=PipelineStats(
            arrival_count=2, completion_count=1,
            mean_latency_seconds=2.0, **ignored_pipe_fields),
        pipelines_interactive=PipelineStats(
            arrival_count=2, completion_count=1,
            mean_latency_seconds=2.0, **ignored_pipe_fields),
        pipelines_batch=PipelineStats(
            arrival_count=2, completion_count=1,
            mean_latency_seconds=2.0, **ignored_pipe_fields),
        **ignored_sim_fields,
    )
    weights = {Priority.QUERY: 1, Priority.INTERACTIVE: 1, Priority.BATCH_PIPELINE: 1}
    result = stats.adjusted_latency(weights=weights, divide_by_completion_rate=False,
                                    unfinished_penalty_seconds=10.0)
    assert result == pytest.approx(6.0)

    # Scenario 8: no completions, penalty=20s.  1 unfinished per category (3 total).
    # result = (20+20+20)/3 = 20.0
    ps_none = PipelineStats(arrival_count=1, completion_count=0,
                            mean_latency_seconds=float('nan'), **ignored_pipe_fields)
    stats = SimulatorStats(
        pipelines_all=PipelineStats(arrival_count=3, completion_count=0,
                                    mean_latency_seconds=float('nan'), **ignored_pipe_fields),
        pipelines_query=ps_none,
        pipelines_interactive=ps_none, pipelines_batch=ps_none,
        **ignored_sim_fields,
    )
    weights = {Priority.QUERY: 1, Priority.INTERACTIVE: 1, Priority.BATCH_PIPELINE: 1}
    result = stats.adjusted_latency(weights=weights, divide_by_completion_rate=False,
                                    unfinished_penalty_seconds=20.0)
    assert result == pytest.approx(20.0)

    # Scenario 9: cannot use both divide_by_completion_rate and penalty
    with pytest.raises(AssertionError):
        stats.adjusted_latency(weights=weights, divide_by_completion_rate=True,
                               unfinished_penalty_seconds=10.0)


@pytest.mark.parametrize("num_pools", [1, 3])
def test_memory_stats(num_pools):
    """Test mean memory allocated/consumed percentages.

    Two pipelines arrive at tick 0 on 10GB pool(s) (overbook scheduler,
    memory overcommit enabled).  Each container gets its full pool RAM as
    allocation but only consumes 5GB.  One op runs 5s, the other 10s.

    With 1 pool (10GB total):
      Seconds 0-4: allocated=200%, consumed=100%
      Seconds 5-9: allocated=100%, consumed=50%
      Mean allocated=150%, mean consumed=75%

    With N pools the percentages are divided by N (total RAM is N*10GB).
    """
    params = get_param_defaults()
    params['scheduler_algo'] = 'overbook'
    params['allow_memory_overcommit'] = True
    params['num_pools'] = num_pools
    params['cpus_per_pool'] = 2
    params['ram_gb_per_pool'] = 10
    params['ticks_per_second'] = 10
    params['duration'] = 10

    p1 = Pipeline("p1", Priority.BATCH_PIPELINE)
    op1 = p1.new_operator()
    op1.add_segment(Segment(baseline_cpu_seconds=5, cpu_scaling="const", memory_gb=5))

    p2 = Pipeline("p2", Priority.BATCH_PIPELINE)
    op2 = p2.new_operator()
    op2.add_segment(Segment(baseline_cpu_seconds=10, cpu_scaling="const", memory_gb=5))

    workload = MockWorkload({0: [p1, p2]}, params['ticks_per_second'])
    stats = run_simulator(params, workload=workload)

    assert stats.containers_completed == 2
    assert stats.mean_memory_allocated_percent == pytest.approx(150.0 / num_pools)
    assert stats.mean_memory_consumed_percent == pytest.approx(75.0 / num_pools)


@pytest.mark.parametrize("max_job_seconds, expected_completions, expected_timeouts", [
    (0, 5, 0),    # no limit: all 5 finish
    (2.5, 2, 3),  # 1s and 2s ops finish; 3s, 4s, 5s are killed
    (4.5, 4, 1),  # 1s, 2s, 3s, 4s finish; 5s is killed
])
def test_max_job_time(max_job_seconds, expected_completions, expected_timeouts):
    """Test that pipelines exceeding max_job_seconds are killed.

    Five pipelines arrive at t=0, each with a single CPU-only op taking
    1s, 2s, 3s, 4s, 5s respectively.  The overbook scheduler assigns all
    concurrently (1 CPU each).  Depending on max_job_seconds, only ops
    that finish before the timeout should count as completed.
    """
    params = get_param_defaults()
    params['scheduler_algo'] = 'overbook'
    params['allow_memory_overcommit'] = True
    params['num_pools'] = 1
    params['cpus_per_pool'] = 5
    params['ram_gb_per_pool'] = 100
    params['ticks_per_second'] = 10
    params['duration'] = 10
    params['max_job_seconds'] = max_job_seconds

    pipelines = []
    for i in range(5):
        p = Pipeline(f"p{i+1}", Priority.BATCH_PIPELINE)
        op = p.new_operator()
        op.add_segment(Segment(baseline_cpu_seconds=i + 1, cpu_scaling="const", storage_read_gb=0))
        pipelines.append(p)

    workload = MockWorkload({0: pipelines}, params['ticks_per_second'])
    stats = run_simulator(params, workload=workload)

    assert stats.pipelines_created == 5
    assert stats.pipelines_all.completion_count == expected_completions
    assert stats.pipelines_all.timeout_count == expected_timeouts
