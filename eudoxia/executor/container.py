import logging
from typing import List, Generator, Optional
from eudoxia.utils import DISK_SCAN_GB_SEC, Priority
from eudoxia.workload import Operator

logger = logging.getLogger(__name__)


class Container:
    """
    An encapsulation of CPU, RAM, and a list of operators. A container executes
    operators tick-by-tick, tracking memory usage and allowing suspension only
    between operator boundaries.
    """
    next_container_num = 1

    def __init__(self, ram, cpu, ops, prty: Priority, pool_id: int, ticks_per_second: int):
        self.container_id = f"c{Container.next_container_num}"
        Container.next_container_num += 1
        self.pool_id = pool_id
        self.ram = ram
        self.cpu = cpu
        self.operators: List[Operator] = ops
        self.suspend_ticks = None
        self._suspend_ticks_left = None
        self.priority = prty
        self.error: Optional[str] = None
        self.ticks_per_second = ticks_per_second
        self.tick_length_secs = 1.0 / ticks_per_second

        # Tick state (updated by generator)
        self._current_memory: float = 0.0
        self._can_suspend: bool = False
        self._completed: bool = False
        self._ticks_elapsed: int = 0

        # Start the generator
        self._tick_iter = self._tick_generator()

    def get_pipeline_id(self):
        """Get the pipeline ID from operators, handling mixed pipelines"""
        pipeline_ids = set()
        for op in self.operators:
            if op.pipeline and op.pipeline.pipeline_id:
                pipeline_ids.add(op.pipeline.pipeline_id)

        if len(pipeline_ids) == 0:
            return "no_pipeline"
        elif len(pipeline_ids) == 1:
            return list(pipeline_ids)[0]
        else:
            return "multiple_pipelines"

    def __repr__(self):
        num_ops = len(self.operators)
        return f"container={self.container_id} pipeline={self.get_pipeline_id()} ops={num_ops} cpus={self.cpu} ram_gb={self.ram}"

    def _tick_generator(self) -> Generator[None, None, None]:
        """
        Generator that drives tick-by-tick execution, updating container state directly.

        Updates self._current_memory and self._can_suspend on each yield.

        Memory usage is determined by the current segment:
        - If segment.memory_gb is set: fixed memory usage
        - If segment.memory_gb is None: memory grows linearly with I/O progress

        Suspension is only allowed between operators (not between segments).
        """

        # loop over every tick of every segment of every op
        #
        # at each iteration, determine memory usage, whether there is
        # an OOM, and whether suspension is possible
        for op_idx, op in enumerate(self.operators):
            segments = op.get_segments()
            for seg_idx, seg in enumerate(segments):
                # Calculate ticks for I/O phase and CPU phase
                io_secs = seg.get_io_seconds()
                cpu_secs = seg.get_cpu_time(self.cpu)
                io_ticks = int(io_secs / self.tick_length_secs)
                cpu_ticks = int(cpu_secs / self.tick_length_secs)
                total_seg_ticks = io_ticks+cpu_ticks

                for i in range(total_seg_ticks):
                    # determine current memory consumption
                    if i < io_ticks:
                        if seg.memory_gb is not None:
                            self._current_memory = seg.memory_gb
                        else:
                            # Memory grows linearly with I/O progress
                            io_progress_secs = (i + 1) * self.tick_length_secs
                            self._current_memory = io_progress_secs * DISK_SCAN_GB_SEC
                    else:
                        self._current_memory = seg.get_peak_memory_gb()

                    # have we OOM'd?
                    if self._current_memory > self.ram:
                        self.error = "OOM"
                        self._completed = True
                        yield
                        return

                    # are we at the end of the op (last tick of last
                    # seg)?  if so, we're either completed, or we can
                    # suspend, depending on whether this is the last
                    # op.
                    self._can_suspend = False
                    if seg_idx == len(segments)-1 and i == total_seg_ticks - 1:
                        if op_idx == len(self.operators) - 1:
                            self._current_memory = 0.0
                            self._completed = True
                        else:
                            self._can_suspend = True
                    yield

    def tick(self):
        """
        Execute one tick. Advances to next state (OOM checked in _advance_tick).
        """
        if self._completed:
            return
        next(self._tick_iter)
        self._ticks_elapsed += 1

    def is_completed(self):
        return self._completed

    def ticks_elapsed(self) -> int:
        """Returns the number of ticks that have been executed."""
        return self._ticks_elapsed

    def can_suspend_container(self) -> bool:
        """Can only suspend a container between operators."""
        return self._can_suspend

    def get_current_memory_usage(self) -> float:
        """Returns current memory usage in GB."""
        return self._current_memory

    def suspend_container(self):
        """
        Suspend container execution, free CPUs and RAM. Requires writing
        current data to disk.
        """
        write_to_disk_secs = self.ram / DISK_SCAN_GB_SEC
        write_to_disk_ticks = int(write_to_disk_secs / self.tick_length_secs)
        self.suspend_ticks = write_to_disk_ticks
        self._suspend_ticks_left = write_to_disk_ticks

    def suspend_container_tick(self):
        self._suspend_ticks_left -= 1

    def is_suspended(self) -> bool:
        return (self._suspend_ticks_left == 0)
