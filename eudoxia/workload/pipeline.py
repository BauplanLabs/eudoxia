from typing import List
import numpy as np
from eudoxia.utils import EudoxiaException, DISK_SCAN_GB_SEC, Priority
from eudoxia.utils.dag import Node, DAG

class ScalingFuncs:
    """
    Different implemented static methods for CPU scaling. Will eventually be
    mapped to a range of CPU/IO values dictating the relative work type of a
    segment 
    """
    @staticmethod
    def _const(num_cpus, init_cpu_time):
        return init_cpu_time

    @staticmethod
    def _linear_bnded_three(num_cpus, init_cpu_time):
        if num_cpus < 3:
            return init_cpu_time / num_cpus
        else:
            return init_cpu_time / 3

    @staticmethod
    def _linear_bnded_seven(num_cpus, init_cpu_time):
        if num_cpus < 7:
            return init_cpu_time / num_cpus
        else:
            return init_cpu_time / 7

    @staticmethod
    def _log_scale(num_cpus, init_cpu_time):
        scaling_factor = np.log(num_cpus) + 1
        return init_cpu_time / scaling_factor

    @staticmethod
    def _sqrt(num_cpus, init_cpu_time):
        scaling_factor = np.sqrt(num_cpus)
        return init_cpu_time / scaling_factor

    @staticmethod
    def _squared(num_cpus, init_cpu_time):
        scaling_factor = np.power(num_cpus, 2)
        return init_cpu_time / scaling_factor

    @staticmethod
    def _exponential_bnd(num_cpus, init_cpu_time):
        scaling_factor = 16
        if num_cpus < 4:
            scaling_factor = np.power(2, num_cpus)
        return init_cpu_time / scaling_factor

class Segment(Node):
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
    def __init__(self, scaling, io=None, init_cpu_time=None, tick_length_secs=None):
        super().__init__()
        self.io = io # GB read into ram
        self.init_cpu_time = init_cpu_time # time for one processor
        self.tick_length_secs = tick_length_secs
        if callable(scaling):
            self.scaling = scaling
            self.scaling_type = "callable"
        elif scaling in Segment.SCALING_FUNCS:
            self.scaling = scaling
            self.scaling_type = "string"
        else:
            raise EudoxiaException("Invalid scaling func passed to segment")

    # can be called by executor to make sure workload gen happened correctly
    def is_initialized(self) -> bool:
        if self.io is not None and self.cpu is not None and self.scaling is not None:
            return True
        return False

    def get_io_time(self):
        return self.io / DISK_SCAN_GB_SEC

    def get_io_ticks(self):
        if self.tick_length_secs is None:
            raise EudoxiaException("tick_length_secs not set for Segment")
        io_time_secs = self.get_io_time()
        io_time_ticks = int(io_time_secs / self.tick_length_secs)
        return io_time_ticks

    def scale_cpu_time(self, num_cpus):
        if self.scaling_type == "callable":
            return self.scaling(num_cpus, self.init_cpu_time)
        else:
            return Segment.SCALING_FUNCS[self.scaling](num_cpus, self.init_cpu_time)

    def scale_cpu_time_ticks(self, num_cpus):
        if self.tick_length_secs is None:
            raise EudoxiaException("tick_length_secs not set for Segment")
        cpu_time_secs = self.scale_cpu_time(num_cpus)
        cpu_time_ticks = int(cpu_time_secs / self.tick_length_secs)
        return cpu_time_ticks



class Operator(Node):
    """ 
    Operator is an encapsulation of many functions or a whole SQL query. Broader
    nodes in the big picture DAG. This is represented as a Tree of Segments
    """
    def __init__(self):
        super().__init__()
        self.values: DAG[Segment] = DAG()

class Pipeline(Node):
    """ 
    Pipeline is the top-level interface that arrives to the scheduler. This is a
    Tree of Operators.
    """
    def __init__(self, priority: Priority):
        super().__init__()
        self.values: DAG[Operator] = DAG()
        self.priority: Priority = priority

