import numpy as np

from .decorators import register_estimator_init, register_estimator
from .estimator import Estimate


@register_estimator_init(key="noisy")
def init_noisy_estimator(e):
    sigma = e.params["noisy_estimator_sigma"]
    if sigma < 0:
        raise ValueError(f"sigma must be >= 0, got {sigma}")
    e.sigma = sigma
    e.rng = np.random.default_rng(e.params["random_seed"])


@register_estimator(key="noisy")
def noisy_estimator(e, op) -> Estimate:
    segments = op.get_segments()
    mem_peak_gb = max(seg.get_peak_memory_gb() for seg in segments)
    if e.sigma > 0:
        # Estimator mistakes are more naturally relative than additive:
        # "20% high" or "2x low" fits memory sizing better than +/- N GB.
        # A lognormal factor is always positive, so multiplicative noise
        # keeps the estimate non-negative while preserving that behavior.
        mem_peak_gb *= e.rng.lognormal(mean=0.0, sigma=e.sigma)
    return Estimate(mem_peak_gb=mem_peak_gb)
