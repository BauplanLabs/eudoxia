from .base import BaseEstimator


class OracleEstimator(BaseEstimator):
    """
    Reads true resource values from Segment ground truth.

    In v1, outputs only mem_peak_gb_est (the peak memory across all segments).
    Additional true values (cpu time, IO time) are intentionally omitted —
    the scheduler does not yet consume them.

    Available ground truth (readable but not output in v1):
      - Peak memory (GB):     max(seg.get_peak_memory_gb())
      - IO time (s):          sum(seg.get_io_seconds())
      - Baseline CPU time (s): sum(seg.baseline_cpu_seconds)
      - Disk read (GB):       sum(seg.storage_read_gb)
    """

    def estimate(self, op, tick: int, context: dict) -> dict:
        segments = op.get_segments()
        mem_peak_gb = max(seg.get_peak_memory_gb() for seg in segments)
        return {
            "mem_peak_gb_est": mem_peak_gb,
        }
