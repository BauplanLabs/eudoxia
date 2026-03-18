import numpy as np

from .decorators import register_estimator
from .estimator import Estimate


class NoisyEstimator:
    """
    Estimator with optional multiplicative noise.

    sigma=0 returns the exact peak memory (no noise).
    sigma>0 applies lognormal noise to model relative error while keeping
    estimates non-negative.
    """

    def __init__(self, sigma: float, seed: int):
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
def noisy_estimator(params: dict) -> NoisyEstimator:
    sigma = params["noisy_estimator_sigma"]
    seed = params.get("estimator_seed", params["random_seed"])
    return NoisyEstimator(sigma=sigma, seed=seed)
