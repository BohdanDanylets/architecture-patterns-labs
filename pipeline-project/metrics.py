import time
import threading
from typing import Dict, List
 
 
class StageMetrics:
    """
    Collects timing data for one pipeline stage.
 
    All mutation methods are protected by a threading.Lock so the
    metrics_queue drain loop (main thread) and any future multi-threaded
    usage remain race-free.
    """
 
    def __init__(self, stage_name: str):
        self.stage_name = stage_name
        self._lock = threading.Lock()
 
        self._processing_times: List[float] = []  # per-item processing durations
        self._items_processed: int = 0
        self._total_blocked_time: float = 0.0      # cumulative wait-on-full-queue time
 
        # Wall-clock start/end set by the orchestrator
        self._start_time: float = 0.0
        self._end_time: float = 0.0
 
    # ------------------------------------------------------------------
    # Mutation API (called from metrics drain loop)
    # ------------------------------------------------------------------
 
    def record_processing_time(self, elapsed: float) -> None:
        with self._lock:
            self._processing_times.append(elapsed)
            self._items_processed += 1
 
    def record_blocked_time(self, elapsed: float) -> None:
        with self._lock:
            self._total_blocked_time += elapsed
 
    def mark_start(self) -> None:
        self._start_time = time.perf_counter()
 
    def mark_end(self) -> None:
        self._end_time = time.perf_counter()
 
    # ------------------------------------------------------------------
    # Read-only properties
    # ------------------------------------------------------------------
 
    @property
    def avg_processing_time(self) -> float:
        """Average CPU time per item in seconds."""
        with self._lock:
            if not self._processing_times:
                return 0.0
            return sum(self._processing_times) / len(self._processing_times)
 
    @property
    def total_processing_time(self) -> float:
        with self._lock:
            return sum(self._processing_times)
 
    @property
    def items_processed(self) -> int:
        with self._lock:
            return self._items_processed
 
    @property
    def wall_clock_time(self) -> float:
        """Elapsed real time from first item to completion."""
        if self._start_time and self._end_time:
            return self._end_time - self._start_time
        return 0.0
 
    @property
    def total_blocked_time(self) -> float:
        with self._lock:
            return self._total_blocked_time
 
    def summary(self) -> Dict:
        return {
            "stage":                   self.stage_name,
            "items_processed":         self.items_processed,
            "avg_processing_time_ms":  self.avg_processing_time * 1000,
            "total_processing_time_s": self.total_processing_time,
            "total_blocked_time_s":    self.total_blocked_time,
            "wall_clock_time_s":       self.wall_clock_time,
        }
 
 
class PipelineMetrics:
    """
    Aggregates StageMetrics for every pipeline stage and provides
    bottleneck detection and a formatted performance report.
    """
 
    def __init__(self):
        self._stages: Dict[str, StageMetrics] = {}
        self._pipeline_start: float = 0.0
        self._pipeline_end: float = 0.0
 
    def get_stage(self, name: str) -> StageMetrics:
        if name not in self._stages:
            self._stages[name] = StageMetrics(name)
        return self._stages[name]
 
    def mark_pipeline_start(self) -> None:
        self._pipeline_start = time.perf_counter()
 
    def mark_pipeline_end(self) -> None:
        self._pipeline_end = time.perf_counter()
 
    @property
    def total_pipeline_time(self) -> float:
        if self._pipeline_start and self._pipeline_end:
            return self._pipeline_end - self._pipeline_start
        return 0.0
 
    def detect_bottleneck(self) -> str:
        """
        Returns the name of the stage with the highest average processing
        time per item. That stage dictates the maximum pipeline throughput
        (the slowest link in the chain).
 
        If multiple stages tie, the last one encountered is returned.
        """
        if not self._stages:
            return "Unknown"
        bottleneck = max(
            self._stages.values(),
            key=lambda s: s.avg_processing_time,
        )
        return bottleneck.stage_name
 
    def print_report(self) -> None:
        """Print a formatted performance report to stdout."""
        separator = "=" * 72
        thin_sep  = "─" * 70
 
        print(f"\n{separator}")
        print("  PIPELINE PERFORMANCE REPORT")
        print(separator)
 
        for stage in self._stages.values():
            s = stage.summary()
            print(f"\n  Stage : {s['stage']}")
            print(f"    Items Processed       : {s['items_processed']}")
            print(f"    Avg Time / Image      : {s['avg_processing_time_ms']:.2f} ms")
            print(f"    Total Processing Time : {s['total_processing_time_s']:.3f} s")
            print(f"    Total Blocked Time    : {s['total_blocked_time_s']:.3f} s")
            print(f"    Wall-Clock Time       : {s['wall_clock_time_s']:.3f} s")
 
        bottleneck = self.detect_bottleneck()
 
        print(f"\n  {thin_sep}")
        print(f"  Total Pipeline Time   : {self.total_pipeline_time:.3f} s")
        print(f"  ⚠  Bottleneck Stage   : *** {bottleneck} ***")
        print(f"{separator}\n")
 