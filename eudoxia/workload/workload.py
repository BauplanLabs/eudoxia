import logging
import numpy as np
import random
from typing import List
from abc import ABC, abstractmethod
from eudoxia.utils import Priority
from .pipeline import Pipeline, Operator, Segment

logger = logging.getLogger(__name__)


class Workload(ABC):
    """Abstract base class for workload sources"""
    
    @abstractmethod
    def run_one_tick(self) -> List[Pipeline]:
        """
        Generate pipelines for the current tick.
        
        Returns:
            List[Pipeline]: List of pipelines arriving at this tick
        """
        pass


class WorkloadGenerator(Workload):
    """
    Class to generate workloads according to user and default parameters
    """
    def __init__(self, waiting_ticks_mean, num_pipelines, num_operators,
                 parallel_factor, num_segs, cpu_io_ratio, 
                 rng: np.random.Generator, batch_prob, query_prob,
                 interactive_prob, tick_length_secs, **kwargs):

        assert cpu_io_ratio <= 1.0 and cpu_io_ratio >= 0, "invalid CPU-IO ratio parameter"
        self.tick_length_secs = tick_length_secs
        self.ticks_since_last_gen = 0
        self.waiting_ticks_mean = waiting_ticks_mean
        self.waiting_ticks_stdev = waiting_ticks_mean / 4
        self.curr_waiting_ticks = 0

        # Random generator seeded with param seed
        self.rng = rng
        # probabilities of interactive, query, batch pipelines
        # normalized so that when cast to np float probs sum to 1, per np notes:
        # https://numpy.org/doc/stable/reference/random/generated/numpy.random.Generator.choice.html
        self.priority_values = [
            Priority.INTERACTIVE.value,
            Priority.QUERY.value,
            Priority.BATCH_PIPELINE.value,
        ]
        prob_array = np.array([
            interactive_prob,
            query_prob,
            batch_prob,
        ])
        self.priority_probs = prob_array / np.sum(prob_array, dtype=float)
        # number of pipelines sent at a time
        self.num_pipelines = num_pipelines
        # average num ops per pipeline
        self.num_operators = num_operators
        # how parallelizable each pipeline is (not currently in use)
        self.parallel_factor = parallel_factor
        # avg num segments per operator
        self.num_segs = num_segs
        # value between 0 and 1 for (on avg) if segments are more CPU or IO heavy (low is IO)
        self.cpu_io_ratio = cpu_io_ratio

    
    def generate_query_segment(self) -> Segment:
        return Segment(baseline_cpu_seconds=15, cpu_scaling="linear3", storage_read_gb=35)

    def generate_segment_not_heavy_io(self) -> Segment:
        """
        Generates a random segment but ensures the segment generated will not be
        in the most IO-heavy category
        """
        val = self.rng.normal(self.cpu_io_ratio)
        if val < -1:
            val = -1
        return self.generate_segment_from_val(val)

    def generate_segment(self) -> Segment:
        """
        Generates a fully random segment per the categories defined
        """
        val = self.rng.normal(self.cpu_io_ratio)
        return self.generate_segment_from_val(val)

    def generate_segment_from_val(self, val) -> Segment:
        """
        Val should be drawn from a normal distribution with standard deviation 1 around
        an average between 0 and 1. Value < -1 is the most IO heavy, and as val
        increases becomes more CPU bound. That involves scanning less data,
        scaling more aggressively, and longer CPU times
        """
        if val < -1:
            return Segment(baseline_cpu_seconds=1, cpu_scaling="const", storage_read_gb=55)
        elif val >= -1 and val < -0.5:
            return Segment(baseline_cpu_seconds=2, cpu_scaling="sqrt", storage_read_gb=55)
        elif val >= -0.5 and val < 0:
            return Segment(baseline_cpu_seconds=5, cpu_scaling="linear3", storage_read_gb=45)
        elif val >= 0 and val < 0.5:
            return Segment(baseline_cpu_seconds=15, cpu_scaling="linear3", storage_read_gb=37.5)
        elif val >= 0.5 and val < 1:
            return Segment(baseline_cpu_seconds=20, cpu_scaling="linear7", storage_read_gb=30)
        elif val >= 1 and val < 1.5:
            return Segment(baseline_cpu_seconds=40, cpu_scaling="linear7", storage_read_gb=20)
        elif val >= 1.5:
            return Segment(baseline_cpu_seconds=80, cpu_scaling="squared", storage_read_gb=10)

    
    # Actual TODO: 1. what logic do we want on failure not enough RAM
    #              2. How frequently are pipelines arriving 
    def generate_pipelines(self):
        """
        Based on initialized parameters, create pipelines. There are parameters
        dictating distributions for size of pipeline, size of operators, amount
        parallelizable, and CPU/IO scaling for segments.
        """
        pipelines = []
        for _ in range(self.num_pipelines):
            priority = self.rng.choice(a=self.priority_values, p=self.priority_probs)
            p = Pipeline(Priority(priority))
            if priority == Priority.QUERY.value:
                op = Operator()
                seg = self.generate_query_segment()
                op.add_segment(seg)
                logger.info(f"Pipeline generated with Priority {Priority(priority)} and 1 op")
                p.values.add_node(op)
            else:
                ops = []
                # TODO: ignoring parallel factor
                curr_num_ops = int(self.rng.normal(self.num_operators,
                                                 self.num_operators/4)) 

                # Normal distribution is continuous and nonzero chance value less
                # than 1 is chosen; this ensures num operators is always at least 1
                if curr_num_ops < 1: 
                    curr_num_ops = 1
                for i in range(curr_num_ops):
                    op = Operator()
                    # Segments are 1:1 with operators in current execution
                    curr_num_segs = 1
                    prev_seg = None
                    for j in range(curr_num_segs):
                        # If first node, make it the most IO bound, else have it
                        # draw randomly from all other segment types
                        if prev_seg is None:
                            seg = self.generate_segment_from_val(-2)
                            op.add_segment(seg)
                        else:
                            seg = self.generate_segment_not_heavy_io()
                            op.add_segment(seg)
                        prev_seg = seg
                    ops.append(op)
                logger.info(f"Pipeline generated with Priority {Priority(priority)} and {curr_num_ops} ops")

                # Pipeline is all operators in a linked list. First call has only
                # one argument as it has no parent. all others have parent that is
                # the previously generated operator in the list
                for i in range(len(ops)):
                    if i == 0:
                        p.values.add_node(ops[i])
                    else:
                        p.values.add_node(ops[i], [ops[i-1]])
            pipelines.append(p)
        return pipelines

    def run_one_tick(self) -> List[Pipeline]:
        """
        In the main run_simulator function, this is one of three core entities
        and this is the function that dictates what the workload generator does
        on a tick. It waits till it's been sufficiently long since it's last
        generation, then generates an appropriate number of pipelines and
        returns those.

        Returns:
            list[pipelines]: pipelines arriving to be scheduled
        """
        if self.ticks_since_last_gen == self.curr_waiting_ticks:
            pipelines = self.generate_pipelines()
            next_wait = int(self.rng.normal(self.waiting_ticks_mean,
                                             self.waiting_ticks_stdev))
            if next_wait <= 0:
                next_wait = self.waiting_ticks_mean
            self.curr_waiting_ticks = next_wait
            self.ticks_since_last_gen = 0
            return pipelines
        else:
            self.ticks_since_last_gen += 1
            return []


