from typing import List, Optional
import numpy as np
from eudoxia.utils import EudoxiaException, DISK_SCAN_GB_SEC, Priority
from eudoxia.utils.dag import Node, DAG
from eudoxia.workload.runtime_status import PipelineRuntimeStatus

class ScalingFuncs:
    """
    Different implemented static methods for CPU scaling. Will eventually be
    mapped to a range of CPU/IO values dictating the relative work type of a
    segment 
    """
    @staticmethod
    def _const(num_cpus, baseline_cpu_seconds):
        return baseline_cpu_seconds

    @staticmethod
    def _linear_bnded_three(num_cpus, baseline_cpu_seconds):
        if num_cpus < 3:
            return baseline_cpu_seconds / num_cpus
        else:
            return baseline_cpu_seconds / 3

    @staticmethod
    def _linear_bnded_seven(num_cpus, baseline_cpu_seconds):
        if num_cpus < 7:
            return baseline_cpu_seconds / num_cpus
        else:
            return baseline_cpu_seconds / 7

    @staticmethod
    def _log_scale(num_cpus, baseline_cpu_seconds):
        scaling_factor = np.log(num_cpus) + 1
        return baseline_cpu_seconds / scaling_factor

    @staticmethod
    def _sqrt(num_cpus, baseline_cpu_seconds):
        scaling_factor = np.sqrt(num_cpus)
        return baseline_cpu_seconds / scaling_factor

    @staticmethod
    def _squared(num_cpus, baseline_cpu_seconds):
        scaling_factor = np.power(num_cpus, 2)
        return baseline_cpu_seconds / scaling_factor

    @staticmethod
    def _exponential_bnd(num_cpus, baseline_cpu_seconds):
        scaling_factor = 16
        if num_cpus < 4:
            scaling_factor = np.power(2, num_cpus)
        return baseline_cpu_seconds / scaling_factor


class Segment:
    """
    The smallest unit of execution, a segment logically is a function in a
    language or a single or small set of SQL operators. This is where logic is
    stored on scaling and how to compute the number of ticks required to execute
    a segment given a container's specs.
    """
    SCALING_FUNCS = {
                        "const":   ScalingFuncs._const,
                        "log":     ScalingFuncs._log_scale,
                        "sqrt":    ScalingFuncs._sqrt,
                        "linear3": ScalingFuncs._linear_bnded_three, 
                        "linear7": ScalingFuncs._linear_bnded_seven, 
                        "squared": ScalingFuncs._squared,
                        "exp":     ScalingFuncs._exponential_bnd,
                    }
    def __init__(self, baseline_cpu_seconds=None, cpu_scaling="const", memory_gb=None, storage_read_gb=0):
        self.baseline_cpu_seconds = baseline_cpu_seconds # time for one processor
        self.memory_gb = memory_gb # GB of memory used (None means linear growth based on storage_read_gb)
        self.storage_read_gb = storage_read_gb # GB read from storage
        if callable(cpu_scaling):
            self.scaling_func = cpu_scaling
        elif cpu_scaling in Segment.SCALING_FUNCS:
            self.scaling_func = Segment.SCALING_FUNCS[cpu_scaling]
        else:
            valid_options = "[<callable>|" + "|".join(Segment.SCALING_FUNCS.keys()) + "]"
            raise EudoxiaException(f"Invalid scaling func passed to segment. Must be one of: {valid_options}")

    def get_io_seconds(self):
        return self.storage_read_gb / DISK_SCAN_GB_SEC

    def get_cpu_time(self, num_cpus):
        return self.scaling_func(num_cpus, self.baseline_cpu_seconds)
    
    def get_peak_memory_gb(self):
        """Get peak memory usage. Defaults to storage_read_gb if memory_gb not specified."""
        if self.memory_gb is not None:
            return self.memory_gb
        return self.storage_read_gb

    def get_seconds_until_oom(self, limit_gb):
        """
        Calculate when this segment will OOM given a memory limit.
        
        Args:
            limit_gb: Memory limit in GB
            
        Returns:
            float: Seconds until OOM occurs, or None if no OOM will occur
            
        Memory usage patterns:
        1. If memory_gb is set: We use a fixed amount of memory from the start.
           - Returns 0.0 if memory_gb > limit_gb (immediate OOM)
           - Returns None if memory_gb <= limit_gb (no OOM)
           
        2. If memory_gb is None: Memory grows linearly as we read from storage.
           - Memory grows at DISK_SCAN_GB_SEC rate
           - OOM occurs when memory usage exceeds limit_gb
           - Returns limit_gb / DISK_SCAN_GB_SEC (time to reach limit)
           - Returns None if peak memory (storage_read_gb) <= limit_gb
        """
        if self.memory_gb is not None:
            # Fixed memory usage pattern
            if self.memory_gb > limit_gb:
                return 0.0  # Immediate OOM
            else:
                return None  # No OOM
        else:
            # Linear memory growth pattern based on storage reads
            if self.storage_read_gb <= limit_gb:
                return None  # No OOM - peak memory within limit
            
            # Calculate when memory usage hits the limit
            # Memory grows at DISK_SCAN_GB_SEC rate, so time to reach limit_gb:
            oom_time = limit_gb / DISK_SCAN_GB_SEC
            return oom_time


class Operator(Node):
    """
    Operator is an encapsulation of many functions or a whole SQL query. Broader
    nodes in the big picture DAG. This is represented as a list of Segments
    """
    def __init__(self, pipeline: 'Pipeline'):
        super().__init__()
        self.values: List[Segment] = []
        self.pipeline: Pipeline = pipeline

    def add_segment(self, segment: Segment):
        """Add a segment to this operator"""
        self.values.append(segment)

    def get_segments(self) -> List[Segment]:
        """Get all segments in this operator"""
        return self.values

    def transition(self, new_state):
        """Transition this operator to a new state."""
        self.pipeline.runtime_status().transition(self, new_state)

    def state(self) -> 'OperatorState':
        """Get the current state of this operator."""
        return self.pipeline.runtime_status().operator_states[self]


class Pipeline(Node):
    """
    Pipeline is the top-level interface that arrives to the scheduler. This is a
    Tree of Operators.

    All fields are immutable except for _runtime_status, which tracks mutable execution state.
    """
    def __init__(self, pipeline_id: str, priority: Priority):
        super().__init__()
        self.pipeline_id: str = pipeline_id
        self.priority: Priority = priority
        self.values: DAG[Operator] = DAG()

        # MUTABLE: Execution state tracking (lazy-initialized via runtime_status() method)
        self._runtime_status: Optional['PipelineRuntimeStatus'] = None

    def runtime_status(self):
        """
        Get or create the runtime status for this pipeline.

        Returns:
            PipelineRuntimeStatus: The runtime status tracking object
        """
        if self._runtime_status is None:
            self._runtime_status = PipelineRuntimeStatus(self)
        return self._runtime_status

    def new_operator(self, parents=None) -> Operator:
        """Create a new operator belonging to this pipeline and add it to the DAG"""
        operator = Operator(self)
        self.values.add_node(operator, parents)
        return operator

