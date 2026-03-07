import numpy as np
from .base import BaseEstimator

# Defensive floor: guards against custom estimator bugs that return
# non-positive values. lognormal(0, sigma) is always positive, so
# this clip will never trigger for the built-in OracleEstimator.
_MIN_POSITIVE = 1e-6


class NoisyEstimator(BaseEstimator):
    """
    Wraps any BaseEstimator and multiplies mem_peak_gb_est by
    lognormal(mu=0, sigma=sigma) noise.

    sigma=0 degenerates to the base estimator (no noise).
    Results are reproducible given the same seed.

    Only mem_peak_gb_est is noised; other keys pass through unchanged.
    """

    def __init__(self, base: BaseEstimator, sigma: float, seed: int):
        if sigma < 0:
            raise ValueError(f"sigma must be >= 0, got {sigma}")
        self.base = base
        self.sigma = sigma
        self.rng = np.random.default_rng(seed)

    def estimate(self, op, tick: int, context: dict) -> dict:
        result = self.base.estimate(op, tick, context)
        if self.sigma > 0:
            noise = self.rng.lognormal(mean=0.0, sigma=self.sigma)
            result["mem_peak_gb_est"] = max(
                _MIN_POSITIVE,
                result["mem_peak_gb_est"] * noise,
            )
        return result
