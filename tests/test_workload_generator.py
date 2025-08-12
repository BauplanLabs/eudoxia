import pytest
import numpy as np
from eudoxia.core.dispatcher import WorkloadGenerator


def test_workload_generator_determinism():
    """
    Test that two WorkloadGenerator instances with the same seed produce
    identical workloads until we get at least 5 cases with pipelines.
    """
    # Common parameters for both generators
    params = {
        'waiting_ticks_mean': 10,
        'num_pipelines': 3,
        'num_operators': 5,
        'parallel_factor': 2,
        'num_segs': 2,
        'cpu_io_ratio': 0.5,
        'batch_prob': 0.4,
        'query_prob': 0.3,
        'interactive_prob': 0.3
    }
    
    # Create two generators with the same seed
    seed = 42
    rng1 = np.random.Generator(np.random.PCG64(seed))
    rng2 = np.random.Generator(np.random.PCG64(seed+1))
    
    gen1 = WorkloadGenerator(rng=rng1, **params)
    gen2 = WorkloadGenerator(rng=rng2, **params)
    
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
