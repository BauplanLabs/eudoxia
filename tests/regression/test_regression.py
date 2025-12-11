"""
Regression test runner for scheduler tests.

This module auto-discovers regression tests under tests/regression/ and runs them.
Each regression test is a directory containing:
  - params.toml: Scheduler configuration
  - trace.csv: Workload trace
  - expected.json: Expected simulation results

To create a new regression test, use:
  eudoxia make-regression-test <params.toml> <target_dir>
"""
import json
import pytest
from pathlib import Path
from eudoxia.simulator import run_simulator, parse_args_with_defaults
from eudoxia.workload.csv_io import CSVWorkloadReader
import tomllib


# Find all regression test directories
REGRESSION_DIR = Path(__file__).parent


def discover_regression_tests():
    """Discover all regression test directories."""
    if not REGRESSION_DIR.exists():
        return []

    tests = []
    for test_dir in sorted(REGRESSION_DIR.iterdir()):
        if not test_dir.is_dir():
            continue

        params_file = test_dir / 'params.toml'
        trace_file = test_dir / 'trace.csv'
        expected_file = test_dir / 'expected.json'

        # Only include directories with all required files
        if params_file.exists() and trace_file.exists() and expected_file.exists():
            tests.append(test_dir.name)

    return tests


REGRESSION_TESTS = discover_regression_tests()


@pytest.mark.parametrize('test_name', REGRESSION_TESTS)
def test_regression(test_name):
    """Run a regression test and compare results to expected output."""
    test_dir = REGRESSION_DIR / test_name
    params_file = test_dir / 'params.toml'
    trace_file = test_dir / 'trace.csv'
    expected_file = test_dir / 'expected.json'

    # Load params
    with open(params_file, 'rb') as f:
        params = tomllib.load(f)
    params = parse_args_with_defaults(params)

    # Load expected results
    with open(expected_file) as f:
        expected = json.load(f)

    # Run simulation with trace
    with open(trace_file) as f:
        reader = CSVWorkloadReader(f)
        workload = reader.get_workload(params['ticks_per_second'])
        stats = run_simulator(params, workload=workload)

    # Convert to dict for comparison
    actual = stats.to_dict()

    # TODO: Add more detailed error messages showing which fields differ
    assert actual == expected, (
        f"Regression test '{test_name}' failed\n"
        f"Expected:\n{json.dumps(expected, indent=2)}\n"
        f"Actual:\n{json.dumps(actual, indent=2)}"
    )
