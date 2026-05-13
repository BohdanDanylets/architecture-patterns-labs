import time
from typing import List
 
from blocking_queue import ThreadSafeBoundedQueue
from producer import TelemetryProducer, POISON_PILL
from consumer import TelemetryConsumer
from metrics import ExperimentResult
from logger import setup_logger
 
logger = setup_logger("Experiment")
 
 
def run_experiment(
    num_producers:    int,
    num_consumers:    int,
    buffer_size:      int,
    total_packets:    int,
    processing_delay: float = 0.0001,
    label:            str   = "",
) -> ExperimentResult:
    """
    Run one Producer-Consumer experiment and return measured metrics.
 
    Args:
        num_producers:    Number of producer threads.
        num_consumers:    Number of consumer threads.
        buffer_size:      ThreadSafeBoundedQueue capacity.
        total_packets:    Total packets to produce across ALL producers.
        processing_delay: Per-packet sleep in consumer (simulates heavy work).
        label:            Human-readable name for reporting.
 
    Returns:
        ExperimentResult with all collected measurements.
    """
    # Divide the total packet quota evenly across producers.
    packets_per_producer = max(1, total_packets // num_producers)
 
    # ── Create shared bounded buffer ─────────────────────────────
    queue = ThreadSafeBoundedQueue(maxsize=buffer_size)
 
    # ── Instantiate threads ───────────────────────────────────────
    consumers: List[TelemetryConsumer] = [
        TelemetryConsumer(
            consumer_id=i,
            queue=queue,
            processing_delay=processing_delay,
        )
        for i in range(num_consumers)
    ]
 
    producers: List[TelemetryProducer] = [
        TelemetryProducer(
            producer_id=i,
            queue=queue,
            production_delay=0.0,   # max production speed
        )
        for i in range(num_producers)
    ]
 
    experiment_start = time.perf_counter()
 
    # ── Start consumers FIRST ─────────────────────────────────────
    # Prevents producers from immediately filling the queue and
    # blocking before any consumer is scheduled.
    for c in consumers:
        c.start()
 
    # ── Start producers ───────────────────────────────────────────
    for p in producers:
        p.start()
 
    logger.info(
        f"  [{label or f'{num_producers}P/{num_consumers}C buf={buffer_size}'}] "
        f"Running — target {total_packets:,} packets "
        f"({packets_per_producer:,}/producer)"
    )
 
    # ── Wait for each producer to meet its quota ──────────────────
    # Polling interval of 5 ms is fine here: we are not in a hot loop.
    for p in producers:
        while p.packets_produced < packets_per_producer:
            if not p.is_alive():
                break
            time.sleep(0.005)
        p.stop()
 
    # Ensure all producer threads have fully exited before we send pills.
    for p in producers:
        p.join(timeout=15)
        if p.is_alive():
            logger.warning(f"  [{p.name}] did not terminate cleanly.")
 
    # ── Send one Poison Pill per consumer ─────────────────────────
    # Consumers will finish draining any remaining real packets from
    # the queue before consuming their pill and exiting.
    for _ in consumers:
        queue.put(POISON_PILL)
 
    # ── Wait for all consumers to drain and terminate ─────────────
    for c in consumers:
        c.join(timeout=120)
        if c.is_alive():
            logger.warning(f"  [{c.name}] did not terminate cleanly.")
 
    elapsed = time.perf_counter() - experiment_start
 
    # ── Collect metrics ───────────────────────────────────────────
    total_consumed = sum(c.packets_consumed for c in consumers)
    queue_stats    = queue.get_stats()
 
    avg_prod_tp = (
        sum(p.packets_produced / max(p.elapsed_time, 1e-9) for p in producers)
        / max(num_producers, 1)
    )
    avg_cons_tp = (
        sum(c.packets_consumed / max(c.elapsed_time, 1e-9) for c in consumers)
        / max(num_consumers, 1)
    )
 
    result = ExperimentResult(
        label                   = label or f"{num_producers}P/{num_consumers}C buf={buffer_size}",
        num_producers           = num_producers,
        num_consumers           = num_consumers,
        buffer_size             = buffer_size,
        total_packets           = total_consumed,
        elapsed_time            = elapsed,
        throughput              = total_consumed / max(elapsed, 1e-9),
        producer_blocks         = queue_stats["producer_blocks"],
        consumer_blocks         = queue_stats["consumer_blocks"],
        avg_producer_throughput = avg_prod_tp,
        avg_consumer_throughput = avg_cons_tp,
    )
 
    logger.info(
        f"  [{result.label}] Done — "
        f"consumed {total_consumed:,} pkts | "
        f"time {elapsed:.3f}s | "
        f"throughput {result.throughput:,.0f} pkt/s | "
        f"prod_blocks={result.producer_blocks} cons_blocks={result.consumer_blocks}"
    )
 
    return result
 