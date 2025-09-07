import pytest
from typing import Dict, List
from eudoxia.simulator import run_simulator, get_param_defaults
from eudoxia.workload import Workload, Pipeline
from eudoxia.utils import Priority

class MockWorkload(Workload):
    """
    Mock workload that returns pre-defined pipelines at specific ticks.
    Useful for testing schedulers with controlled workloads.
    """
    
    def __init__(self, workload_schedule: Dict[int, List[Pipeline]]):
        """
        Args:
            workload_schedule: Dict mapping tick number to list of pipelines to return
        """
        self.workload_schedule = workload_schedule
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
    params['duration'] = 400 * params['tick_length_secs']

    # simple workload with 3 jobs, arriving once every 100 ticks
    pipelines = []
    for i in range(3):
        pipeline = Pipeline(Priority.BATCH_PIPELINE)
        op = Operator()
        seg = Segment(baseline_cpu_seconds=5, cpu_scaling="linear3", storage_read_gb=10)
        op.add_segment(seg)
        pipeline.values.add_node(op)
        pipelines.append(pipeline)

    workload = MockWorkload({
        100: [pipelines[0]],
        200: [pipelines[1]],
        300: [pipelines[2]],
    })

    # run simulator and check results
    stats = run_simulator(params, workload=workload)
    assert stats.pipelines_created == 3, f"{scheduler_algo}: Expected 3 created pipelines, got {stats.pipelines_created}"

    # TODO: can we calculate how long we need to way to see them finish?
    #assert stats.pipelines_completed == 3, f"{scheduler_algo}: Expected 3 completed pipelines, got {stats.pipelines_completed}"


@pytest.mark.parametrize("tick_length_secs", [10e-6, 100e-6, 1e-3, 10e-3])  # 10us, 100us, 1ms, 10ms
def test_tick_length(tick_length_secs):
    """Test that tick length affects execution timing correctly"""
    from eudoxia.workload import Operator, Segment

    # Configure for precise timing test
    params = get_param_defaults()
    params.update({
        'scheduler_algo': 'naive',
        'num_pools': 1,
        'cpu_pool': 1,  # Only 1 CPU so jobs run sequentially
        'duration': 4.9,  # 4.9 seconds - should allow only 4 jobs to complete
        'tick_length_secs': tick_length_secs,
    })

    # Create 10 pipelines that arrive immediately
    pipelines = []
    for i in range(10):
        pipeline = Pipeline(Priority.BATCH_PIPELINE)
        op = Operator()
        # CPU-only segment: 0 IO, 1 second CPU time
        seg = Segment(baseline_cpu_seconds=1.0, cpu_scaling="const", storage_read_gb=0)
        op.add_segment(seg)
        pipeline.values.add_node(op)
        pipelines.append(pipeline)

    # All pipelines arrive at tick 0
    workload_schedule = {0: pipelines}
    mock_workload = MockWorkload(workload_schedule)

    # Run simulation
    stats = run_simulator(params, workload=mock_workload)
    
    # Verify: 10 jobs created, but only 4 complete in 4.9 seconds
    # (each job takes 1 second, and with 1 CPU they run sequentially)
    assert stats.pipelines_created == 10, f"Expected 10 pipelines created, got {stats.pipelines_created}"
    assert stats.pipelines_completed == 4, f"Expected 4 pipelines completed, got {stats.pipelines_completed}"
