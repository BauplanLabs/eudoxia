class SimClock:
    def __init__(self, ticks_per_second: int):
        self.ticks_per_second = ticks_per_second
        self._current_tick = -1

    def tick(self):
        self._current_tick += 1

    def now_ticks(self) -> int:
        return self._current_tick

    def now_seconds(self) -> float:
        return self._current_tick / self.ticks_per_second
