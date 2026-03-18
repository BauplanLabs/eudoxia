import pytest
from eudoxia.workload.pipeline import Pipeline, Segment
from eudoxia.estimator import ESTIMATOR_ALGOS, Estimate, NoisyEstimator, Estimator
from eudoxia.utils import Priority


def _make_op(segments):
    """Create an Operator with given segments inside a minimal Pipeline."""
    pipeline = Pipeline("test_pipeline", Priority.BATCH_PIPELINE)
    op = pipeline.new_operator()
    for seg in segments:
        op.add_segment(seg)
    return op


# ── NoisyEstimator (sigma=0 behaves like oracle) ──────────

class TestNoisyEstimator:

    def test_memory_gb_set(self):
        """Returns explicit memory_gb as mem_peak_gb_est."""
        op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                               memory_gb=32.0, storage_read_gb=50)])
        est = NoisyEstimator(sigma=0.0, seed=42).estimate(op)
        assert est.mem_peak_gb_est == 32.0

    def test_memory_gb_none_fallback(self):
        """Falls back to storage_read_gb when memory_gb is None."""
        op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                               memory_gb=None, storage_read_gb=55)])
        est = NoisyEstimator(sigma=0.0, seed=42).estimate(op)
        assert est.mem_peak_gb_est == 55.0

    def test_multiple_segments_takes_max(self):
        """Takes max across segments."""
        op = _make_op([
            Segment(baseline_cpu_seconds=5, cpu_scaling="const", memory_gb=10, storage_read_gb=0),
            Segment(baseline_cpu_seconds=5, cpu_scaling="const", memory_gb=50, storage_read_gb=0),
            Segment(baseline_cpu_seconds=5, cpu_scaling="const", memory_gb=30, storage_read_gb=0),
        ])
        est = NoisyEstimator(sigma=0.0, seed=42).estimate(op)
        assert est.mem_peak_gb_est == 50.0


# ── Noise behavior ───────────────────────────────────────────────

class TestNoisyBehavior:

    def test_sigma_zero_matches_oracle(self):
        """sigma=0 is exact oracle behavior."""
        op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                               memory_gb=42.0, storage_read_gb=0)])
        assert NoisyEstimator(sigma=0.0, seed=123).estimate(op).mem_peak_gb_est == \
               NoisyEstimator(sigma=0.0, seed=42).estimate(op).mem_peak_gb_est

    def test_same_seed_reproducible(self):
        """Same seed → same noisy result."""
        op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                               memory_gb=42.0, storage_read_gb=0)])
        n1 = NoisyEstimator(sigma=1.0, seed=42)
        n2 = NoisyEstimator(sigma=1.0, seed=42)
        assert n1.estimate(op).mem_peak_gb_est == n2.estimate(op).mem_peak_gb_est

    def test_different_seed_different_results(self):
        """Different seed → different noisy result."""
        op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                               memory_gb=42.0, storage_read_gb=0)])
        n1 = NoisyEstimator(sigma=1.0, seed=1)
        n2 = NoisyEstimator(sigma=1.0, seed=999)
        assert n1.estimate(op).mem_peak_gb_est != n2.estimate(op).mem_peak_gb_est

    def test_negative_sigma_raises(self):
        """sigma < 0 raises ValueError at construction time."""
        with pytest.raises(ValueError, match="sigma must be >= 0"):
            NoisyEstimator(sigma=-0.5, seed=42)


# ── Estimator factory ──────────────────────────────────────

class TestEstimator:

    def test_none_algo_is_noop(self):
        """estimator_algo=None → Estimator exists but estimate() is a no-op."""
        est = Estimator(estimator_algo=None)
        assert est._algo is None

    def test_noisyoracle_algo_name(self):
        est = Estimator(
            estimator_algo="noisyoracle",
            noisy_estimator_sigma=0.5,
            random_seed=42,
        )
        assert isinstance(est._algo, NoisyEstimator)

    def test_noisyoracle_is_registered(self):
        assert "noisyoracle" in ESTIMATOR_ALGOS

    def test_unknown_algo_raises(self):
        """Unknown estimator_algo raises ValueError."""
        with pytest.raises(ValueError, match="Unknown estimator_algo"):
            Estimator(estimator_algo="magic", random_seed=42)


# ── Operator.estimate + to_dict() ────────────────────────────────

class TestOperatorEstimateField:

    def test_default_estimate_empty_and_serialized(self):
        """New op has an empty Estimate, and to_dict() includes None field."""
        pipeline = Pipeline("p1", Priority.BATCH_PIPELINE)
        op = pipeline.new_operator()
        op.add_segment(Segment(baseline_cpu_seconds=1, cpu_scaling="const", storage_read_gb=10))
        assert op.estimate == Estimate()
        assert op.to_dict()["estimate"] == {"mem_peak_gb_est": None}

    def test_to_dict_after_estimator(self):
        """to_dict() reflects estimator-injected values."""
        pipeline = Pipeline("p1", Priority.BATCH_PIPELINE)
        op = pipeline.new_operator()
        op.add_segment(Segment(baseline_cpu_seconds=1, cpu_scaling="const",
                               memory_gb=42.0, storage_read_gb=10))
        op.estimate = NoisyEstimator(sigma=0.0, seed=42).estimate(op)
        assert op.to_dict()["estimate"]["mem_peak_gb_est"] == 42.0
