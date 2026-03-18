from dataclasses import dataclass

from .decorators import ESTIMATOR_ALGOS


@dataclass
class Estimate:
    """
    Scheduling-visible hints written by an estimator.

    `None` means estimator has not provided that hint.
    """

    mem_peak_gb_est: float | None = None

    def to_dict(self) -> dict:
        return {"mem_peak_gb_est": self.mem_peak_gb_est}


class Estimator:
    """
    A modular class which can utilize any implemented estimator algorithm.

    Mirrors the design of Scheduler. When estimator_algo is None, estimate()
    is a no-op and op.estimate stays as its default empty Estimate.

    Relevant params:
      - estimator_algo (str|None): None or "noisyoracle".
      - noisy_estimator_sigma (float): lognormal noise sigma (default 0.0 = no noise).
      - estimator_seed (int): RNG seed. Fallback chain: estimator_seed → random_seed.
    """

    def __init__(self, estimator_algo, **params):
        if estimator_algo is None:
            self._algo = None
        elif estimator_algo not in ESTIMATOR_ALGOS:
            raise ValueError(f"Unknown estimator_algo: {repr(estimator_algo)}")
        else:
            self._algo = ESTIMATOR_ALGOS[estimator_algo](params)

    def estimate(self, op) -> None:
        if self._algo is not None:
            op.estimate = self._algo.estimate(op)
