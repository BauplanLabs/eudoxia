import numpy as np

from .decorators import register_estimator
from .estimate import Estimate


class NoisyOracleEstimator:
    """
    Oracle estimator with optional multiplicative noise.

    sigma=0 is exact oracle behavior.
    sigma>0 applies lognormal noise to model relative error while keeping
    estimates non-negative.
    """

    def __init__(self, sigma: float = 0.0, seed: int = 42):
        if sigma < 0:
            raise ValueError(f"sigma must be >= 0, got {sigma}")
        self.sigma = sigma
        self.rng = np.random.default_rng(seed)

    def estimate(self, op) -> Estimate:
        segments = op.get_segments()
        mem_peak_gb = max(seg.get_peak_memory_gb() for seg in segments)
        if self.sigma > 0:
            # Estimator mistakes are more naturally relative than additive:
            # "20% high" or "2x low" fits memory sizing better than +/- N GB.
            # A lognormal factor is always positive, so multiplicative noise
            # keeps the estimate non-negative while preserving that behavior.
            mem_peak_gb *= self.rng.lognormal(mean=0.0, sigma=self.sigma)
        return Estimate(mem_peak_gb_est=mem_peak_gb)


@register_estimator(key="noisyoracle")
def build_noisy_oracle_estimator(params: dict) -> NoisyOracleEstimator:
    sigma = params.get("estimator_noise_sigma", 0.0)
    seed = params.get("estimator_seed", params.get("random_seed", 42))
    return NoisyOracleEstimator(sigma=sigma, seed=seed)
