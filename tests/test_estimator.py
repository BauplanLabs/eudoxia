import pytest
from eudoxia.workload.pipeline import Pipeline, Segment
from eudoxia.estimator import OracleEstimator, NoisyEstimator, build_estimator
from eudoxia.utils import Priority


def _make_op(segments):
    """Create an Operator with given segments inside a minimal Pipeline."""
    pipeline = Pipeline("test_pipeline", Priority.BATCH_PIPELINE)
    op = pipeline.new_operator()
    for seg in segments:
        op.add_segment(seg)
    return op


# ── OracleEstimator ──────────────────────────────────────────────

class TestOracleEstimator:

    def test_memory_gb_set(self):
        """Returns explicit memory_gb as mem_peak_gb_est."""
        op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                               memory_gb=32.0, storage_read_gb=50)])
        est = OracleEstimator().estimate(op, tick=0, context={})
        assert est["mem_peak_gb_est"] == 32.0

    def test_memory_gb_none_fallback(self):
        """Falls back to storage_read_gb when memory_gb is None."""
        op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                               memory_gb=None, storage_read_gb=55)])
        est = OracleEstimator().estimate(op, tick=0, context={})
        assert est["mem_peak_gb_est"] == 55.0

    def test_multiple_segments_takes_max(self):
        """Takes max across segments."""
        op = _make_op([
            Segment(baseline_cpu_seconds=5, cpu_scaling="const", memory_gb=10, storage_read_gb=0),
            Segment(baseline_cpu_seconds=5, cpu_scaling="const", memory_gb=50, storage_read_gb=0),
            Segment(baseline_cpu_seconds=5, cpu_scaling="const", memory_gb=30, storage_read_gb=0),
        ])
        est = OracleEstimator().estimate(op, tick=0, context={})
        assert est["mem_peak_gb_est"] == 50.0


# ── NoisyEstimator ───────────────────────────────────────────────

class TestNoisyEstimator:

    def test_sigma_zero_matches_oracle(self):
        """sigma=0 produces identical results to base oracle."""
        op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                               memory_gb=42.0, storage_read_gb=0)])
        oracle = OracleEstimator()
        noisy = NoisyEstimator(oracle, sigma=0.0, seed=123)
        assert noisy.estimate(op, 0, {})["mem_peak_gb_est"] == \
               oracle.estimate(op, 0, {})["mem_peak_gb_est"]

    def test_same_seed_reproducible(self):
        """Same seed → same noisy result."""
        op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                               memory_gb=42.0, storage_read_gb=0)])
        n1 = NoisyEstimator(OracleEstimator(), sigma=1.0, seed=42)
        n2 = NoisyEstimator(OracleEstimator(), sigma=1.0, seed=42)
        assert n1.estimate(op, 0, {})["mem_peak_gb_est"] == \
               n2.estimate(op, 0, {})["mem_peak_gb_est"]

    def test_different_seed_different_results(self):
        """Different seed → different noisy result."""
        op = _make_op([Segment(baseline_cpu_seconds=10, cpu_scaling="const",
                               memory_gb=42.0, storage_read_gb=0)])
        n1 = NoisyEstimator(OracleEstimator(), sigma=1.0, seed=1)
        n2 = NoisyEstimator(OracleEstimator(), sigma=1.0, seed=999)
        assert n1.estimate(op, 0, {})["mem_peak_gb_est"] != \
               n2.estimate(op, 0, {})["mem_peak_gb_est"]

    def test_negative_sigma_raises(self):
        """sigma < 0 raises ValueError at construction time."""
        with pytest.raises(ValueError, match="sigma must be >= 0"):
            NoisyEstimator(OracleEstimator(), sigma=-0.5, seed=42)

    def test_noisy_result_is_positive(self):
        """Noisy estimate is always positive (defensive clip)."""
        op = _make_op([Segment(baseline_cpu_seconds=1, cpu_scaling="const",
                               memory_gb=0.001, storage_read_gb=0)])
        noisy = NoisyEstimator(OracleEstimator(), sigma=2.0, seed=42)
        assert noisy.estimate(op, 0, {})["mem_peak_gb_est"] > 0


# ── build_estimator factory ──────────────────────────────────────

class TestBuildEstimator:

    def test_none_algo_returns_none(self):
        """estimator_algo=None → returns None (no estimator)."""
        assert build_estimator({"estimator_algo": None}) is None

    def test_oracle_algo(self):
        """estimator_algo='oracle' → returns OracleEstimator."""
        est = build_estimator({"estimator_algo": "oracle", "random_seed": 42})
        assert isinstance(est, OracleEstimator)

    def test_oracle_with_noise(self):
        """sigma > 0 wraps oracle in NoisyEstimator."""
        est = build_estimator({
            "estimator_algo": "oracle",
            "estimator_noise_sigma": 0.5,
            "random_seed": 42,
        })
        assert isinstance(est, NoisyEstimator)

    def test_unknown_algo_raises(self):
        """Unknown estimator_algo raises ValueError."""
        with pytest.raises(ValueError, match="Unknown estimator_algo"):
            build_estimator({"estimator_algo": "magic", "random_seed": 42})


# ── Operator.estimate + to_dict() ────────────────────────────────

class TestOperatorEstimateField:

    def test_default_estimate_empty_and_serialized(self):
        """New op has estimate={}, and to_dict() includes it."""
        pipeline = Pipeline("p1", Priority.BATCH_PIPELINE)
        op = pipeline.new_operator()
        op.add_segment(Segment(baseline_cpu_seconds=1, cpu_scaling="const", storage_read_gb=10))
        assert op.estimate == {}
        assert op.to_dict()["estimate"] == {}

    def test_to_dict_after_estimator(self):
        """to_dict() reflects estimator-injected values."""
        pipeline = Pipeline("p1", Priority.BATCH_PIPELINE)
        op = pipeline.new_operator()
        op.add_segment(Segment(baseline_cpu_seconds=1, cpu_scaling="const",
                               memory_gb=42.0, storage_read_gb=10))
        op.estimate = OracleEstimator().estimate(op, tick=0, context={})
        assert op.to_dict()["estimate"]["mem_peak_gb_est"] == 42.0
