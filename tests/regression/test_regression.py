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
import math
import pytest
from pathlib import Path
from eudoxia.simulator import run_simulator, parse_args_with_defaults
from eudoxia.workload.csv_io import CSVWorkloadReader
import tomllib


def dict_diff(expected, actual, path=""):
    """
    Recursively compare two dicts and return a list of differences.

    For floats, uses math.isclose for comparison.
    Returns a list of human-readable difference strings.
    """
    differences = []

    # Check if both are dicts
    if not isinstance(expected, dict) or not isinstance(actual, dict):
        if expected != actual:
            differences.append(f"{path}: expected {expected!r}, got {actual!r}")
        return differences

    expected_keys = set(expected.keys())
    actual_keys = set(actual.keys())

    # Keys only in expected
    for key in expected_keys - actual_keys:
        key_path = f"{path}.{key}" if path else key
        differences.append(f"{key_path}: missing in actual (expected {expected[key]!r})")

    # Keys only in actual
    for key in actual_keys - expected_keys:
        key_path = f"{path}.{key}" if path else key
        differences.append(f"{key_path}: unexpected key in actual (got {actual[key]!r})")

    # Keys in both - compare values
    for key in expected_keys & actual_keys:
        key_path = f"{path}.{key}" if path else key
        exp_val = expected[key]
        act_val = actual[key]

        if isinstance(exp_val, dict) and isinstance(act_val, dict):
            # Recurse for nested dicts
            differences.extend(dict_diff(exp_val, act_val, key_path))
        elif isinstance(exp_val, float) and isinstance(act_val, float):
            # Use math.isclose for floats, handle nan specially
            if math.isnan(exp_val) and math.isnan(act_val):
                pass  # Both nan is considered equal
            elif not math.isclose(exp_val, act_val):
                differences.append(f"{key_path}: expected {exp_val}, got {act_val}")
        elif exp_val != act_val:
            differences.append(f"{key_path}: expected {exp_val!r}, got {act_val!r}")

    return differences


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

    # Get detailed diff for error message
    differences = dict_diff(expected, actual)
    if differences:
        diff_msg = "\n".join(f"  - {d}" for d in differences)
        pytest.fail(
            f"Regression test '{test_name}' failed\n"
            f"Differences:\n{diff_msg}"
        )
