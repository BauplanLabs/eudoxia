from dataclasses import dataclass


@dataclass
class Estimate:
    """
    Scheduling-visible hints written by an estimator.

    `None` means estimator has not provided that hint.
    """

    mem_peak_gb_est: float | None = None

    def to_dict(self) -> dict:
        return {"mem_peak_gb_est": self.mem_peak_gb_est}
