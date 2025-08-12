import pytest
import numpy as np
from unittest.mock import Mock, patch
from eudoxia.main import get_param_defaults, run_simulator
from eudoxia.core.workload import WorkloadGenerator
from eudoxia.utils import Priority


def test_query_distribution_priorities():
    """
    Test that different probability distributions produce the correct pipeline priorities.
    Tests 100% of each type (interactive, query, batch) separately.
    """
    
    # Number of pipelines to generate
    N = 10
    
    test_cases = [
        # (param_key, expected_priority_value)
        ("interactive_prob", Priority.INTERACTIVE.value),
        ("query_prob", Priority.QUERY.value),
        ("batch_prob", Priority.BATCH_PIPELINE.value),
    ]

    for param_key, expected_priority in test_cases:
        params = get_param_defaults()  # Get fresh params for each test case
        params["interactive_prob"] = 0
        params["query_prob"] = 0
        params["batch_prob"] = 0
        params[param_key] = 1    # 100% this type, nothing else
        params["waiting_ticks_mean"] = 1  # Generate pipelines frequently
        params["num_pipelines"] = 2  # Generate 2 per event
        params["rng"] = np.random.default_rng(params["random_seed"])
        gen = WorkloadGenerator(**params)

        tick = 0
        max_ticks = 10000  # Safety limit
        pipeline_count = 0

        while pipeline_count < N and tick < max_ticks:
            pipelines = gen.run_one_tick()
            for pipeline in pipelines:
                pipeline_count += 1
                assert pipeline.priority.value == expected_priority, \
                    f"Pipeline for {param_key} has priority {pipeline.priority.value}, " \
                    f"expected {expected_priority}"
            tick += 1

        assert pipeline_count >= N
