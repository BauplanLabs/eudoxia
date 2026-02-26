from typing import Dict, List
from eudoxia.simulator import run_simulator, get_param_defaults
from eudoxia.workload import Workload, Pipeline, OperatorState
from eudoxia.workload.pipeline import Segment
from eudoxia.utils import Priority


class MockWorkload(Workload):
    """
    Mock workload that returns pre-defined pipelines at specific times.
    """

    def __init__(self, workload_schedule: Dict[float, List[Pipeline]], ticks_per_second: int):
        self.workload_schedule = {}
        for arrival_seconds, pipelines in workload_schedule.items():
            tick = int(arrival_seconds * ticks_per_second)
            self.workload_schedule[tick] = pipelines
        self.current_tick = 0

    def run_one_tick(self) -> List[Pipeline]:
        pipelines = self.workload_schedule.get(self.current_tick, [])
        self.current_tick += 1
        return pipelines


def test_check_locality_parent_pool_mismatch():
    pipeline = Pipeline("p1", Priority.BATCH_PIPELINE)
    parent = pipeline.new_operator()
    child = pipeline.new_operator([parent])

    status = pipeline.runtime_status()
    parent.transition(OperatorState.ASSIGNED)
    parent.transition(OperatorState.RUNNING)
    parent.transition(OperatorState.COMPLETED)
    status.operator_status[parent].pool_id = 0

    ok, err = status.check_locality(child, 1)
    assert not ok
    assert "Locality violation" in err

    ok, err = status.check_locality(child, 0)
    assert ok
    assert err == ""


def test_record_operator_pool_stores_pool_id():
    pipeline = Pipeline("p1", Priority.BATCH_PIPELINE)
    op = pipeline.new_operator()
    status = pipeline.runtime_status()
    status.operator_status[op].pool_id = 2
    assert status.operator_status[op].pool_id == 2


def test_simulation_without_locality_enforcement_has_no_locality_errors():
    params = get_param_defaults()
    params.update({
        "scheduler_algo": "naive",
        "num_pools": 1,
        "cpus_per_pool": 1,
        "ram_gb_per_pool": 10,
        "ticks_per_second": 10,
        "duration": 2.0,
        "enforce_data_locality": False,
    })

    pipeline = Pipeline("p1", Priority.BATCH_PIPELINE)
    op = pipeline.new_operator()
    op.add_segment(Segment(
        baseline_cpu_seconds=0.1,
        cpu_scaling="const",
        memory_gb=1,
        storage_read_gb=0,
    ))

    workload = MockWorkload({0.0: [pipeline]}, params["ticks_per_second"])
    stats = run_simulator(params, workload=workload)

    for error in stats.failure_error_counts.keys():
        assert "Locality violation" not in error
