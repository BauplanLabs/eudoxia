from dataclasses import dataclass

from .decorators import INIT_ALGOS, ESTIMATOR_ALGOS


@dataclass
class Estimate:
    """
    Scheduling-visible hints written by an estimator.

    `None` means estimator has not provided that hint.
    """

    mem_peak_gb: float | None = None

    def to_dict(self) -> dict:
        return {"mem_peak_gb": self.mem_peak_gb}


class Estimator:
    """
    A modular class which can utilize any implemented estimator algorithm. It
    accepts operators from the Simulator and writes scheduling-visible hints
    based on the algorithm its utilizing. It provides these hints to the
    Scheduler via op.estimate.
    """

    def __init__(self, estimator_algo, **params):
        self.params = params
        if estimator_algo is None:
            self._algo_func = None
        elif estimator_algo not in ESTIMATOR_ALGOS:
            options = sorted(ESTIMATOR_ALGOS.keys())
            raise ValueError(f"Unknown estimator_algo: {repr(estimator_algo)}. Options: {options}")
        else:
            self._algo_func = ESTIMATOR_ALGOS[estimator_algo]
            INIT_ALGOS[estimator_algo](self)

    def estimate(self, op) -> None:
        if self._algo_func is not None:
            op.estimate = self._algo_func(self, op)
