import pytest
from eudoxia.workload import WorkloadGenerator
from eudoxia.simulator import get_param_defaults


def _generate_interactive_pipelines(dag_linear_prob, dag_branch_in_prob, dag_branch_out_prob):
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
        "dag_branch_out_prob": dag_branch_out_prob,
    })
    gen = WorkloadGenerator(**params)
    return gen.generate_pipelines()


def _is_linear_dag(pipeline):
    ops = list(pipeline.values)
    if not ops:
        return False
    if len(ops[0].parents) != 0:
        return False
    for i in range(1, len(ops)):
        if ops[i].parents != [ops[i - 1]]:
            return False
    return True


def _is_branch_in_dag(pipeline):
    ops = list(pipeline.values)
    if not ops:
        return False
    if len(ops) == 1:
        return len(ops[0].parents) == 0

    join_op = ops[-1]
    if len(join_op.parents) != len(ops) - 1 or len(join_op.children) != 0:
        return False
    for op in ops[:-1]:
        if len(op.parents) != 0:
            return False
        if op.children != [join_op]:
            return False
    return True

def _is_branch_out_dag(pipeline):
    ops = list(pipeline.values)
    if not ops:
        return False
    if len(ops) == 1:
        return len(ops[0].parents) == 0

    join_op = ops[0]
    if len(join_op.parents) != 0 or len(join_op.children) != len(ops) - 1:
        return False
    for op in ops[1:]:
        if len(op.parents) != 1:
            return False
        if len(op.children) != 0:
            return False
    return True
    

    


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


def test_dag_linear_structure():
    pipelines = _generate_interactive_pipelines(dag_linear_prob=1.0, dag_branch_in_prob=0.0, dag_branch_out_prob=0.0)
    assert len(pipelines) == 10
    for p in pipelines:
        assert _is_linear_dag(p), f"Expected linear DAG, got structure with {len(list(p.values))} ops"


def test_dag_branch_in_structure():
    pipelines = _generate_interactive_pipelines(dag_linear_prob=0.0, dag_branch_in_prob=1.0, dag_branch_out_prob=0.0)
    assert len(pipelines) == 10
    for p in pipelines:
        assert _is_branch_in_dag(p), f"Expected branch-in DAG, got structure with {len(list(p.values))} ops"


def test_dag_branch_out_structure():
    pipelines = _generate_interactive_pipelines(dag_linear_prob=0.0, dag_branch_in_prob=0.0, dag_branch_out_prob=1.0)
    assert len(pipelines) == 10
    for p in pipelines:
        assert _is_branch_out_dag(p), f"Expected branch-out DAG, got structure with {len(list(p.values))} ops"