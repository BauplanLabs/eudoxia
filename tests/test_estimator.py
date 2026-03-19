import pytest
from eudoxia.workload.pipeline import Pipeline, Segment
from eudoxia.estimator import Estimate, Estimator
from eudoxia.utils import Priority


def _make_op(segments):
    """Create an Operator with given segments inside a minimal Pipeline."""
    pipeline = Pipeline("test_pipeline", Priority.BATCH_PIPELINE)
    op = pipeline.new_operator()
    for seg in segments:
        op.add_segment(seg)
    return op


def _make_estimator(sigma=0.0, seed=42):
    """Create an Estimator with noisyoracle algo."""
    return Estimator(
        estimator_algo="noisyoracle",
        noisy_estimator_sigma=sigma,
        random_seed=seed,
    )


def test_none_algo_leaves_estimate_unchanged():
    """estimator_algo=None → estimate() leaves op.estimate as default."""
    estimator = Estimator(estimator_algo=None)
    op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                           memory_gb=32.0, storage_read_gb=50)])
    estimator.estimate(op)
    assert op.estimate == Estimate()


def test_unknown_algo_raises():
    """Unknown estimator_algo raises ValueError."""
    with pytest.raises(ValueError, match="Unknown estimator_algo"):
        Estimator(estimator_algo="magic", random_seed=42)


def test_estimate_returns_peak_memory():
    """Estimator populates mem_peak_gb_est from segment memory_gb."""
    estimator = _make_estimator()
    op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                           memory_gb=32.0, storage_read_gb=50)])
    estimator.estimate(op)
    assert op.estimate.mem_peak_gb_est == 32.0


def test_estimate_falls_back_to_storage_read():
    """When memory_gb is None, falls back to storage_read_gb."""
    estimator = _make_estimator()
    op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                           memory_gb=None, storage_read_gb=55)])
    estimator.estimate(op)
    assert op.estimate.mem_peak_gb_est == 55.0


def test_estimate_takes_max_across_segments():
    """With multiple segments, estimate is max peak memory."""
    estimator = _make_estimator()
    op = _make_op([
        Segment(baseline_cpu_seconds=5, cpu_scaling="const", memory_gb=10, storage_read_gb=0),
        Segment(baseline_cpu_seconds=5, cpu_scaling="const", memory_gb=50, storage_read_gb=0),
        Segment(baseline_cpu_seconds=5, cpu_scaling="const", memory_gb=30, storage_read_gb=0),
    ])
    estimator.estimate(op)
    assert op.estimate.mem_peak_gb_est == 50.0


def test_noise_changes_estimate():
    """sigma>0 → estimate differs from true peak memory."""
    estimator = _make_estimator(sigma=1.0)
    op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                           memory_gb=42.0, storage_read_gb=0)])
    estimator.estimate(op)
    assert op.estimate.mem_peak_gb_est != 42.0
    assert op.estimate.mem_peak_gb_est > 0


def test_consecutive_estimates_get_independent_noise():
    """Same estimator called on identical ops produces different noise each time."""
    estimator = _make_estimator(sigma=1.0)
    op1 = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                            memory_gb=42.0, storage_read_gb=0)])
    op2 = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                            memory_gb=42.0, storage_read_gb=0)])
    estimator.estimate(op1)
    estimator.estimate(op2)
    assert op1.estimate.mem_peak_gb_est != op2.estimate.mem_peak_gb_est


def test_same_seed_produces_same_estimate():
    """Same seed → same noisy result."""
    op1 = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                            memory_gb=42.0, storage_read_gb=0)])
    op2 = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                            memory_gb=42.0, storage_read_gb=0)])
    _make_estimator(sigma=1.0, seed=42).estimate(op1)
    _make_estimator(sigma=1.0, seed=42).estimate(op2)
    assert op1.estimate.mem_peak_gb_est == op2.estimate.mem_peak_gb_est


def test_different_seed_produces_different_estimate():
    """Different seed → different noisy result."""
    op1 = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                            memory_gb=42.0, storage_read_gb=0)])
    op2 = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                            memory_gb=42.0, storage_read_gb=0)])
    _make_estimator(sigma=1.0, seed=1).estimate(op1)
    _make_estimator(sigma=1.0, seed=999).estimate(op2)
    assert op1.estimate.mem_peak_gb_est != op2.estimate.mem_peak_gb_est


def test_negative_sigma_raises():
    """sigma < 0 raises ValueError."""
    with pytest.raises(ValueError, match="sigma must be >= 0"):
        _make_estimator(sigma=-0.5)


def test_default_estimate_serializes_with_none():
    """New op has empty Estimate, to_dict() includes None field."""
    pipeline = Pipeline("p1", Priority.BATCH_PIPELINE)
    op = pipeline.new_operator()
    op.add_segment(Segment(baseline_cpu_seconds=1, cpu_scaling="const", storage_read_gb=10))
    assert op.estimate == Estimate()
    assert op.to_dict()["estimate"] == {"mem_peak_gb_est": None}


def test_to_dict_reflects_estimate():
    """to_dict() reflects estimator-injected values."""
    estimator = _make_estimator()
    pipeline = Pipeline("p1", Priority.BATCH_PIPELINE)
    op = pipeline.new_operator()
    op.add_segment(Segment(baseline_cpu_seconds=1, cpu_scaling="const",
                           memory_gb=42.0, storage_read_gb=10))
    estimator.estimate(op)
    assert op.to_dict()["estimate"]["mem_peak_gb_est"] == 42.0
