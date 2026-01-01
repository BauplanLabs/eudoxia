import logging
from typing import List, Generator, Optional
from eudoxia.utils import DISK_SCAN_GB_SEC, Priority
from eudoxia.workload import Operator, OperatorState
from eudoxia.executor.assignment import Assignment

logger = logging.getLogger(__name__)


class Container:
    """
    An encapsulation of CPU, RAM, and a list of operators. A container executes
    operators tick-by-tick, tracking memory usage and allowing suspension only
    between operator boundaries.
    """
    next_container_num = 1

    def __init__(self, assignment: Assignment, pool, ticks_per_second: int):
        self.container_id = f"c{Container.next_container_num}"
        Container.next_container_num += 1
        self.assignment = assignment
        self.pool = pool
        self.suspend_ticks = None
        self._suspend_ticks_left = None
        self.error: Optional[str] = None
        self.ticks_per_second = ticks_per_second
        self.tick_length_secs = 1.0 / ticks_per_second

        # Tick state (updated by generator)
        self._current_memory: float = 0.0
        self._can_suspend: bool = False
        self._completed: bool = False
        self._ticks_elapsed: int = 0
        # Index of current/next operator. Ops before this index are COMPLETED.
        self._current_op_idx: int = 0

        # Start the generator
        self._tick_iter = self._tick_generator()

    @property
    def operators(self) -> List[Operator]:
        return self.assignment.ops

    @property
    def pool_id(self) -> int:
        return self.pool.pool_id

    @property
    def priority(self) -> Priority:
        return self.assignment.priority

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
        return f"container={self.container_id} pipeline={self.get_pipeline_id()} ops={num_ops} cpus={self.assignment.cpu} ram_gb={self.assignment.ram}"

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
            # Transition to RUNNING when we start this operator
            op.transition(OperatorState.RUNNING)

            segments = op.get_segments()
            for seg_idx, seg in enumerate(segments):
                # Calculate ticks for I/O phase and CPU phase
                io_secs = seg.get_io_seconds()
                cpu_secs = seg.get_cpu_time(self.assignment.cpu)
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
                    if self._current_memory > self.assignment.ram:
                        self.error = "OOM"

                        # if the current running op fails, the ops
                        # before it are completed, and the ones after
                        # it are assigned.
                        #
                        # TODO: decide when an op fails, what should
                        # be the state of the other ops in the
                        # container be?
                        #
                        # for example, does an op that was already
                        # completed need to be retried, or was the
                        # output checkpointed somewhere?
                        for remaining_op in self.operators[op_idx:]:
                            remaining_op.transition(OperatorState.FAILED)
                        self._current_memory = 0.0
                        self._completed = True
                        yield
                        return

                    # are we at the end of the op (last tick of last
                    # seg)?  if so, we're either completed, or we can
                    # suspend, depending on whether this is the last
                    # op.
                    self._can_suspend = False
                    if seg_idx == len(segments)-1 and i == total_seg_ticks - 1:
                        # Operator completed successfully
                        op.transition(OperatorState.COMPLETED)
                        self._current_op_idx += 1
                        if op_idx == len(self.operators) - 1:
                            self._current_memory = 0.0
                            self._completed = True
                        else:
                            self._can_suspend = True
                    yield

    def tick(self):
        """
        Execute one tick. Advances to next state (OOM checked in _advance_tick).
        Updates pool's consumed memory with the delta from this tick.
        """
        if self._completed:
            return
        old_memory = self._current_memory
        next(self._tick_iter)
        self._ticks_elapsed += 1
        delta = self._current_memory - old_memory
        self.pool.consumed_ram_gb += delta

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

    def kill(self, error: str = "OOM"):
        """Kill this container externally (e.g., due to pool-level OOM).

        Marks the container as completed with an error, transitions remaining
        ops to FAILED, and updates pool's consumed memory.

        Note: This is separate from the internal OOM handling in the generator
        because the memory accounting differs. Internal OOM happens mid-tick
        and tick() handles the delta; external kill happens after all ticks
        complete and must update consumed_ram_gb directly.
        """
        if self._completed:
            return
        self.error = error
        # Transition remaining ops to FAILED (same as internal OOM handling)
        for op in self.operators[self._current_op_idx:]:
            op.transition(OperatorState.FAILED)
        old_memory = self._current_memory
        self._current_memory = 0.0
        self._completed = True
        self.pool.consumed_ram_gb += (self._current_memory - old_memory)

    def suspend_container(self):
        """
        Suspend container execution, free CPUs and RAM. Requires writing
        current data to disk.
        """
        write_to_disk_secs = self.assignment.ram / DISK_SCAN_GB_SEC
        write_to_disk_ticks = int(write_to_disk_secs / self.tick_length_secs)
        self.suspend_ticks = write_to_disk_ticks
        self._suspend_ticks_left = write_to_disk_ticks

        # Transition remaining operators (ASSIGNED) to SUSPENDING
        # Ops before _current_op_idx are already COMPLETED
        for op in self.operators[self._current_op_idx:]:
            op.transition(OperatorState.SUSPENDING)

    def suspend_container_tick(self):
        self._suspend_ticks_left -= 1
        if self._suspend_ticks_left == 0:
            # Suspension complete - transition ops back to PENDING for re-assignment
            for op in self.operators[self._current_op_idx:]:
                op.transition(OperatorState.PENDING)

    def is_suspended(self) -> bool:
        return (self._suspend_ticks_left == 0)
