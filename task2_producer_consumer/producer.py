import math
import random
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional
 
from blocking_queue import ThreadSafeBoundedQueue
from logger import setup_logger
 
logger = setup_logger("Producer")
 
POISON_PILL = None
 
 
@dataclass(frozen=True)
class TelemetryPacket:
    """
    Immutable telemetry reading from a remote IoT sensor.
 
    Fields:
        packet_id:   Short UUID for tracing.
        sensor_id:   Simulated sensor label (e.g. 'SENSOR_042').
        timestamp:   Unix epoch at creation time.
        value:       Simulated sensor measurement (Gaussian around 50).
        producer_id: Which TelemetryProducer created this packet.
    """
 
    packet_id:   str   = field(default_factory=lambda: uuid.uuid4().hex[:8])
    sensor_id:   str   = ""
    timestamp:   float = field(default_factory=time.time)
    value:       float = 0.0
    producer_id: int   = 0
 
 
class TelemetryProducer(threading.Thread):
    """
    Continuously generates TelemetryPacket objects and enqueues them.
 
    Args:
        producer_id:      Unique integer identifier.
        queue:            Shared ThreadSafeBoundedQueue instance.
        num_sensors:      Size of the simulated sensor pool (for sensor_id variety).
        production_delay: Seconds to sleep between packets (0 = max speed).
    """
 
    def __init__(
        self,
        producer_id: int,
        queue: ThreadSafeBoundedQueue,
        num_sensors: int = 20,
        production_delay: float = 0.0,
    ) -> None:
        super().__init__(name=f"Producer-{producer_id:02d}", daemon=True)
        self.producer_id      = producer_id
        self._queue           = queue
        self._num_sensors     = num_sensors
        self._production_delay = production_delay
 
        self._stop_event = threading.Event()
 
        self.packets_produced:    int   = 0
        self.total_blocked_time:  float = 0.0  
        self._start_time: Optional[float] = None
        self._end_time:   Optional[float] = None
 
 
    def stop(self) -> None:
        """
        Signal this producer to stop after the current iteration.
        Thread-safe: threading.Event.set() is atomic.
        """
        self._stop_event.set()
 
    def run(self) -> None:
        self._start_time = time.perf_counter()
        logger.debug(f"[{self.name}] Started (sensors={self._num_sensors})")
 
        while not self._stop_event.is_set():
            packet = TelemetryPacket(
                sensor_id=f"SENSOR_{random.randint(1, self._num_sensors):03d}",
                timestamp=time.time(),
                value=round(random.gauss(50.0, 15.0), 4),
                producer_id=self.producer_id,
            )
 
            t0 = time.perf_counter()
            success = self._queue.put(packet)
            blocked_duration = time.perf_counter() - t0
 
            if success:
                self.packets_produced   += 1
                self.total_blocked_time += blocked_duration
 
            if self._production_delay > 0:
                time.sleep(self._production_delay)
 
        self._end_time = time.perf_counter()
        logger.debug(
            f"[{self.name}] Stopped — produced {self.packets_produced} packets"
        )
 
 
    @property
    def elapsed_time(self) -> float:
        """Wall-clock duration from start to stop."""
        if self._start_time and self._end_time:
            return self._end_time - self._start_time
        return max(time.perf_counter() - (self._start_time or 0), 1e-9)
 
    def get_stats(self) -> dict:
        elapsed = self.elapsed_time
        return {
            "producer_id":       self.producer_id,
            "packets_produced":  self.packets_produced,
            "elapsed_time_s":    round(elapsed, 4),
            "throughput_pkt_s":  round(self.packets_produced / max(elapsed, 1e-9), 1),
            "total_blocked_s":   round(self.total_blocked_time, 4),
        }
 