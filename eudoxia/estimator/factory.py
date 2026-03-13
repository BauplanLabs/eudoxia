from .decorators import ESTIMATOR_ALGOS

# Import estimator modules so decorators run at import time.
from . import oracle  # noqa: F401


def build_estimator(params: dict):
    """
    Build an estimator from simulator params.

    Returns None if estimator_algo is None (no estimation).

    Relevant params:
      - estimator_algo (str|None): None or "noisyoracle".
      - estimator_noise_sigma (float): lognormal noise sigma (default 0.0 = no noise).
      - estimator_seed (int): RNG seed. Fallback chain: estimator_seed → random_seed → 42.
    """
    algo = params.get("estimator_algo", None)
    if algo is None:
        return None

    if algo not in ESTIMATOR_ALGOS:
        raise ValueError(f"Unknown estimator_algo: {repr(algo)}")

    return ESTIMATOR_ALGOS[algo](params)
