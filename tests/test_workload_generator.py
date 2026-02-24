import pytest
import numpy as np
from eudoxia.workload import WorkloadGenerator
from eudoxia.simulator import get_param_defaults


def _generate_non_query_pipelines(dag_linear_prob, dag_branch_in_prob):
    params = get_param_defaults()
    params.update({
        "random_seed": 42,
        "waiting_seconds_mean": 0.001,
        "num_pipelines": 10,
        "num_operators": 5,
        "num_segs": 1,
        "cpu_io_ratio": 0.5,
        "interactive_prob": 1.0,
        "query_prob": 0.0,
        "batch_prob": 0.0,
        "dag_linear_prob": dag_linear_prob,
        "dag_branch_in_prob": dag_branch_in_prob,
    })
    gen = WorkloadGenerator(**params)
    return gen.generate_pipelines()


def _is_linear_dag(pipeline):
    ops = list(pipeline.values)
    if not ops:
        return False
    roots = [op for op in ops if len(op.parents) == 0]
    if len(roots) != 1:
        return False
    if any(len(op.parents) > 1 for op in ops):
        return False
    if any(len(op.children) > 1 for op in ops):
        return False
    return True


def _is_branch_in_dag(pipeline):
    ops = list(pipeline.values)
    if not ops:
        return False
    if len(ops) == 1:
        return len(ops[0].parents) == 0 and len(ops[0].children) == 0

    join_ops = [op for op in ops if len(op.parents) > 0]
    if len(join_ops) != 1:
        return False
    join_op = join_ops[0]
    if len(join_op.parents) != len(ops) - 1 or len(join_op.children) != 0:
        return False

    root_ops = [op for op in ops if len(op.parents) == 0]
    if len(root_ops) != len(ops) - 1:
        return False
    return all(len(op.children) == 1 and op.children[0] is join_op for op in root_ops)


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


def test_dag_shape_linear_when_branch_in_prob_zero():
    pipelines = _generate_non_query_pipelines(dag_linear_prob=1.0, dag_branch_in_prob=0.0)
    assert len(pipelines) == 10
    for p in pipelines:
        assert _is_linear_dag(p), f"Expected linear DAG, got structure with {len(list(p.values))} ops"


def test_dag_shape_branch_in_when_branch_in_prob_one():
    pipelines = _generate_non_query_pipelines(dag_linear_prob=0.0, dag_branch_in_prob=1.0)
    assert len(pipelines) == 10
    for p in pipelines:
        assert _is_branch_in_dag(p), f"Expected branch-in DAG, got structure with {len(list(p.values))} ops"
