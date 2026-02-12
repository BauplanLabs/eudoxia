import pytest
import numpy as np
from eudoxia.workload import WorkloadGenerator
from eudoxia.simulator import get_param_defaults
from eudoxia.utils import Priority


def test_workload_generator_determinism():
    """
    Test that two WorkloadGenerator instances with the same seed produce
    identical workloads until we get at least 5 cases with pipelines.
    """
    # Common parameters for both generators
    params = get_param_defaults()
    params.update({
        'waiting_seconds_mean': 0.001,
        'num_pipelines': 3,
        'num_operators': 5,
        'num_segs': 1,
        'cpu_io_ratio': 0.5,
        'batch_prob': 0.4,
        'query_prob': 0.3,
        'interactive_prob': 0.3
    })
    
    # Create two generators with the same seed
    seed = 42
    params['random_seed'] = seed
    
    gen1 = WorkloadGenerator(**params)
    gen2 = WorkloadGenerator(**params)
    
    # Run until we get at least 5 cases where pipelines are generated
    pipeline_cases = 0
    tick = 0
    max_ticks = 10000  # Safety limit
    
    while pipeline_cases < 5 and tick < max_ticks:
        pipelines1 = gen1.run_one_tick()
        pipelines2 = gen2.run_one_tick()
        
        # Check same number of pipelines
        assert len(pipelines1) == len(pipelines2), \
            f"Tick {tick}: different number of pipelines {len(pipelines1)} vs {len(pipelines2)}"
        
        # If pipelines were generated, check they match
        if len(pipelines1) > 0:
            pipeline_cases += 1
            
            for i, (p1, p2) in enumerate(zip(pipelines1, pipelines2)):
                # Check priority
                assert p1.priority == p2.priority, \
                    f"Tick {tick}, Pipeline {i}: different priorities {p1.priority} vs {p2.priority}"
                
                # Check number of operator nodes
                num_ops1 = len(p1.values.node_ids)
                num_ops2 = len(p2.values.node_ids)
                assert num_ops1 == num_ops2, \
                    f"Tick {tick}, Pipeline {i}: different number of operators {num_ops1} vs {num_ops2}"
        
        tick += 1
    
    assert pipeline_cases >= 5, f"Only got {pipeline_cases} pipeline generation events in {tick} ticks"


def test_dag_shape_probabilities_must_be_non_negative():
    params = get_param_defaults()
    params.update({
        'dag_linear_prob': -0.1,
        'dag_branch_in_prob': 1.1,
    })

    with pytest.raises(AssertionError, match="non-negative"):
        WorkloadGenerator(**params)


def test_default_dag_shape_does_not_advance_rng_stream():
    """
    When DAG shape is deterministic (linear=1, branch_in=0), workload generation
    should not consume an extra RNG draw for shape selection.
    """
    params = get_param_defaults()
    params.update({
        'random_seed': 123,
        'num_pipelines': 1,
        'interactive_prob': 1.0,
        'query_prob': 0.0,
        'batch_prob': 0.0,
        'dag_linear_prob': 1.0,
        'dag_branch_in_prob': 0.0,
    })

    # Simulate the pre-DAG-shape random draws for one run_one_tick call.
    rng = np.random.default_rng(params['random_seed'])
    priority_values = [
        Priority.INTERACTIVE.value,
        Priority.QUERY.value,
        Priority.BATCH_PIPELINE.value,
    ]
    priority_probs = np.array([
        params['interactive_prob'],
        params['query_prob'],
        params['batch_prob'],
    ])
    priority_probs = priority_probs / np.sum(priority_probs, dtype=float)
    rng.choice(a=priority_values, p=priority_probs)  # priority draw
    rng.normal(params['num_operators'], params['num_operators'] / 4)  # num_ops draw

    waiting_ticks_mean = int(params['waiting_seconds_mean'] * params['ticks_per_second'])
    waiting_ticks_stdev = waiting_ticks_mean / 4
    expected_next_wait = int(rng.normal(waiting_ticks_mean, waiting_ticks_stdev))
    if expected_next_wait <= 0:
        expected_next_wait = waiting_ticks_mean

    gen = WorkloadGenerator(**params)
    pipelines = gen.run_one_tick()

    assert len(pipelines) == 1
    assert gen.curr_waiting_ticks == expected_next_wait
