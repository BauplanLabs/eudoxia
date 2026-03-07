from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eudoxia.workload.pipeline import Operator


class BaseEstimator(ABC):
    """
    Pluggable estimator interface.

    Before the scheduler sees a newly-arrived operator, the simulator calls
    estimate() to populate op.estimate with scheduling-visible hints.

    The estimator and scheduler are intentionally decoupled:
      - estimate() may return {} to signal "no prediction available".
      - Schedulers that use estimates must handle missing keys via .get()
        and fall back to their default logic.
      - run_simulator(estimator=None) skips estimation entirely;
        op.estimate stays {} and the scheduler runs without hints.

    Convention (not enforced):
      - "mem_peak_gb_est" (float): estimated peak memory in GB
      - Additional keys may be added freely; unknown keys are ignored.
    """

    @abstractmethod
    def estimate(self, op: "Operator", tick: int, context: dict) -> dict:
        """
        Estimate scheduling-visible metrics for a single operator.

        Args:
            op:      Newly-arrived operator. Access true values via op.get_segments().
            tick:    Current simulation tick.
            context: {"params": dict} — simulator params, read-only.

        Returns:
            dict with at minimum: {"mem_peak_gb_est": float}
        """
        ...
