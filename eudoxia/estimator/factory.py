from .base import BaseEstimator
from .oracle import OracleEstimator
from .noisy import NoisyEstimator


def build_estimator(params: dict) -> BaseEstimator | None:
    """
    Build a BaseEstimator from simulator params.

    Returns None if estimator_algo is None (no estimation).

    Relevant params:
      - estimator_algo (str|None):     None (default, no estimator) or "oracle"
      - estimator_noise_sigma (float): 0.0 (default, no noise)
      - estimator_seed (int):          defaults to random_seed
    """
    algo = params.get("estimator_algo", None)
    if algo is None:
        return None

    sigma = params.get("estimator_noise_sigma", 0.0)
    seed = params.get("estimator_seed", params.get("random_seed", 42))

    if algo == "oracle":
        base = OracleEstimator()
    else:
        raise ValueError(f"Unknown estimator_algo: {repr(algo)}")

    if sigma > 0:
        return NoisyEstimator(base, sigma=sigma, seed=seed)
    return base
