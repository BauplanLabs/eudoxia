import pytest
from typing import Dict, List
from eudoxia.main import run_simulator, get_param_defaults
from eudoxia.core.workload import Workload
from eudoxia.utils import Pipeline, Priority
from eudoxia.utils.consts import TICK_LENGTH_SECS


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
    from eudoxia.utils import Operator, Segment

    # configure scheduler
    params = get_param_defaults()
    params['scheduler_algo'] = scheduler_algo
    if scheduler_algo == "priority-pool":
        params['num_pools'] = 2  # priority-pool scheduler requires 2 pools
    params['duration'] = 400 * TICK_LENGTH_SECS

    # simple workload with 3 jobs, arriving once every 100 ticks
    pipelines = []
    for i in range(3):
        pipeline = Pipeline(Priority.BATCH_PIPELINE)
        op = Operator()
        seg = Segment("linear3", 10, 5)
        op.values.add_node(seg)
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
