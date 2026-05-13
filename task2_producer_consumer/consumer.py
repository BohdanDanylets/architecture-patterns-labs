import math
import threading
import time
from typing import List, Optional
 
from blocking_queue import ThreadSafeBoundedQueue
from producer import TelemetryPacket, POISON_PILL
from logger import setup_logger
 
logger = setup_logger("Consumer")
 
 
class TelemetryConsumer(threading.Thread):
    """
    Consumes TelemetryPacket objects and performs aggregation:
      1. Running sample mean  (μ)
      2. Running population std-dev (σ)
      3. Exponential Moving Average (EMA)
      4. Artificial CPU-bound loop (simulates decryption / validation)
 
    Args:
        consumer_id:      Unique integer identifier.
        queue:            Shared ThreadSafeBoundedQueue instance.
        processing_delay: Extra sleep in seconds added per packet
                          to simulate I/O or heavy computation.
    """
 
    def __init__(
        self,
        consumer_id: int,
        queue: ThreadSafeBoundedQueue,
        processing_delay: float = 0.0001,
    ) -> None:
        super().__init__(name=f"Consumer-{consumer_id:02d}", daemon=True)
        self.consumer_id       = consumer_id
        self._queue            = queue
        self._processing_delay = processing_delay
 
        # ── Aggregation state (private to this thread — no locking needed) ──
        self._values: List[float] = []
        self._ema:    float       = 0.0
        self._ema_alpha: float    = 0.1   # smoothing factor
 
        # ── Analytics ────────────────────────────────────────────────
        self.packets_consumed:       int   = 0
        self.total_processing_time:  float = 0.0  # CPU time spent in _process()
        self.total_blocked_time:     float = 0.0  # time waiting in queue.get()
        self._start_time: Optional[float] = None
        self._end_time:   Optional[float] = None
 
    # ------------------------------------------------------------------
    # Private processing logic
    # ------------------------------------------------------------------
 
    def _process_packet(self, packet: TelemetryPacket) -> tuple:
        """
        Simulates CPU-heavy telemetry aggregation.
 
        Steps:
          1. Accumulate value into running list.
          2. Compute sample mean μ = Σv / n
          3. Compute population variance σ² = Σ(v-μ)² / n  →  σ = √σ²
          4. Update EMA: ema = α·v + (1-α)·ema
          5. CPU burn loop: sum of √i for i in [1, 500]
             (simulates decryption / CRC validation)
          6. Optional sleep (configurable I/O simulation)
 
        Returns:
            (mean, std_dev) as floats.
 
        Thread safety:
          All state (_values, _ema) is private to THIS thread instance —
          no synchronisation is needed within this method.
        """
        v = packet.value
        self._values.append(v)
        n = len(self._values)
 
        # Running mean
        mean = sum(self._values) / n
 
        # Running std-dev (population formula)
        variance = sum((x - mean) ** 2 for x in self._values) / n
        std_dev  = math.sqrt(variance) if variance > 0 else 0.0
 
        # Exponential Moving Average
        self._ema = self._ema_alpha * v + (1.0 - self._ema_alpha) * self._ema
 
        # CPU-bound artificial work — simulates checksum / decryption
        _ = sum(math.sqrt(i) for i in range(1, 500))
 
        # Simulated I/O / heavy computation delay
        if self._processing_delay > 0:
            time.sleep(self._processing_delay)
 
        return mean, std_dev
 
    # ------------------------------------------------------------------
    # Thread entry point
    # ------------------------------------------------------------------
 
    def run(self) -> None:
        self._start_time = time.perf_counter()
        logger.debug(f"[{self.name}] Started")
 
        while True:
            # ── Block until an item is available ──────────────────────
            t_get = time.perf_counter()
            item  = self._queue.get()   # Releases CPU while waiting
            self.total_blocked_time += time.perf_counter() - t_get
 
            # ── Poison Pill → graceful shutdown ───────────────────────
            if item is POISON_PILL:
                logger.debug(
                    f"[{self.name}] Received Poison Pill — shutting down "
                    f"(consumed {self.packets_consumed} packets)."
                )
                break
 
            # ── Process the packet ────────────────────────────────────
            packet: TelemetryPacket = item
 
            t_proc = time.perf_counter()
            mean, std_dev = self._process_packet(packet)
            proc_elapsed  = time.perf_counter() - t_proc
 
            self.packets_consumed      += 1
            self.total_processing_time += proc_elapsed
 
            # Periodic debug log (every 1 000 packets to avoid flooding)
            if self.packets_consumed % 1_000 == 0:
                logger.debug(
                    f"[{self.name}] Consumed {self.packets_consumed:>7} | "
                    f"EMA={self._ema:6.2f} | μ={mean:6.2f} | σ={std_dev:6.2f}"
                )
 
        self._end_time = time.perf_counter()
        logger.debug(
            f"[{self.name}] Finished — consumed {self.packets_consumed} packets"
        )
 
    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------
 
    @property
    def elapsed_time(self) -> float:
        if self._start_time and self._end_time:
            return self._end_time - self._start_time
        return max(time.perf_counter() - (self._start_time or 0), 1e-9)
 
    def get_stats(self) -> dict:
        elapsed = self.elapsed_time
        return {
            "consumer_id":           self.consumer_id,
            "packets_consumed":      self.packets_consumed,
            "elapsed_time_s":        round(elapsed, 4),
            "throughput_pkt_s":      round(self.packets_consumed / max(elapsed, 1e-9), 1),
            "avg_processing_ms":     round(
                (self.total_processing_time / max(self.packets_consumed, 1)) * 1000, 3
            ),
            "total_blocked_s":       round(self.total_blocked_time, 4),
        }
 