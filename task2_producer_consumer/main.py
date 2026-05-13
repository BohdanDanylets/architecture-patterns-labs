import argparse
import sys
from pathlib import Path
 
sys.path.insert(0, str(Path(__file__).parent))
 
from metrics import MetricsCollector, ExperimentResult
from utils import run_experiment
from logger import setup_logger
 
logger = setup_logger("Main")
 
 
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Producer-Consumer Experiment Suite (Task 2)"
    )
    p.add_argument(
        "--packets",
        type=int,
        default=10_000,
        help="Total packets per experiment (default: 10 000)",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=0.0001,
        help="Per-packet processing delay in consumers, seconds (default: 0.0001)",
    )
    return p.parse_args()
 
 
# ══════════════════════════════════════════════════════════════════════
#  Experiment 1 — vary Producer/Consumer ratio
# ══════════════════════════════════════════════════════════════════════
 
def run_experiment1(
    total_packets:    int,
    processing_delay: float,
    collector:        MetricsCollector,
) -> list:
    """
    Fixed buffer size (100).  Vary the P:C ratio:
      1P / 10C   → producer-limited (consumers often idle)
      5P /  5C   → balanced
      10P /  1C  → consumer-limited (producers often blocked)
    """
    logger.info("")
    logger.info("=" * 65)
    logger.info("  EXPERIMENT 1  —  Producer / Consumer Ratio")
    logger.info(f"  Buffer size fixed at 100  |  packets = {total_packets:,}")
    logger.info("=" * 65)
 
    BUFFER_SIZE = 100
    configs = [
        (1,  10, "1P / 10C"),
        (5,   5, "5P /  5C"),
        (10,  1, "10P / 1C"),
    ]
 
    results = []
    for np_, nc, label in configs:
        r = run_experiment(
            num_producers    = np_,
            num_consumers    = nc,
            buffer_size      = BUFFER_SIZE,
            total_packets    = total_packets,
            processing_delay = processing_delay,
            label            = label,
        )
        results.append(r)
        collector.record(r)
 
    collector.print_experiment1_table(results)
    return results
 
 
# ══════════════════════════════════════════════════════════════════════
#  Experiment 2 — vary buffer size
# ══════════════════════════════════════════════════════════════════════
 
def run_experiment2(
    total_packets:    int,
    processing_delay: float,
    collector:        MetricsCollector,
) -> list:
    """
    Fixed 5P / 5C.  Vary the buffer (bounded queue) capacity:
      1, 10, 100, 1000 elements.
 
    Expected observations:
      - Buffer=1   → producers constantly wait; throughput collapses.
      - Buffer=10  → moderate blocking; some improvement.
      - Buffer=100 → producers rarely block; throughput peaks.
      - Buffer=1000→ near-zero producer blocks; throughput plateaus
                      (now limited by consumer processing speed).
    """
    logger.info("")
    logger.info("=" * 65)
    logger.info("  EXPERIMENT 2  —  Buffer Size Impact  (5P / 5C)")
    logger.info(f"  packets = {total_packets:,}")
    logger.info("=" * 65)
 
    configs = [1, 10, 100, 1_000]
 
    results = []
    for buf in configs:
        r = run_experiment(
            num_producers    = 5,
            num_consumers    = 5,
            buffer_size      = buf,
            total_packets    = total_packets,
            processing_delay = processing_delay,
            label            = f"buf={buf}",
        )
        results.append(r)
        collector.record(r)
 
    collector.print_experiment2_table(results)
    return results
 
 
# ══════════════════════════════════════════════════════════════════════
#  Conclusions
# ══════════════════════════════════════════════════════════════════════
 
def print_conclusions(exp1: list, exp2: list) -> None:
    best1  = max(exp1, key=lambda r: r.throughput)
    worst1 = min(exp1, key=lambda r: r.throughput)
    best2  = max(exp2, key=lambda r: r.throughput)
 
    # Check whether consumer blocks dominate in 1P/10C
    one_p_ten_c = next((r for r in exp1 if r.num_producers == 1), None)
    ten_p_one_c = next((r for r in exp1 if r.num_consumers == 1), None)
 
    print("\n" + "=" * 72)
    print("  CONCLUSIONS  (for written report)")
    print("=" * 72)
 
    print(f"""
  Experiment 1 — Producer/Consumer Ratio
  ───────────────────────────────────────
  Best  throughput : {best1.label:<14} → {best1.throughput:>10,.0f} pkt/s
  Worst throughput : {worst1.label:<14} → {worst1.throughput:>10,.0f} pkt/s""")
 
    if one_p_ten_c:
        print(f"""
  1P / 10C analysis:
    With a single producer and ten consumers, the system is PRODUCER-LIMITED.
    Consumer threads block frequently on an empty queue
    (consumer_blocks = {one_p_ten_c.consumer_blocks:,}).
    The single producer cannot generate packets fast enough to keep all
    ten consumers busy simultaneously — nine consumers are idle on average.""")
 
    if ten_p_one_c:
        print(f"""
  10P / 1C analysis:
    With ten producers and a single consumer, the system is CONSUMER-LIMITED.
    Producer threads block frequently on a full queue
    (producer_blocks = {ten_p_one_c.producer_blocks:,}).
    The single consumer is the bottleneck; producers must wait for it
    to dequeue before they can insert the next packet (backpressure in action).""")
 
    print(f"""
  5P / 5C analysis:
    The balanced configuration avoids both producer saturation and consumer
    starvation.  Context-switch overhead is minimised because threads spend
    less time blocked.  This typically yields the highest throughput.
 
  Experiment 2 — Buffer Size
  ───────────────────────────
  Best throughput  : {best2.label:<14} → {best2.throughput:>10,.0f} pkt/s""")
 
    for r in exp2:
        ratio = r.producer_blocks / max(r.consumer_blocks + r.producer_blocks, 1) * 100
        print(
            f"    buf={r.buffer_size:<6}  throughput={r.throughput:>9,.0f} pkt/s  "
            f"prod_blocks={r.producer_blocks:>6}  cons_blocks={r.consumer_blocks:>6}  "
            f"(prod blocking ratio {ratio:5.1f}%)"
        )
 
    print("""
  Interpretation:
    A buffer of size 1 forces strict lock-step alternation between every
    put() and get() — maximum synchronisation overhead, minimum throughput.
    Increasing the buffer allows producers to burst-fill the queue during
    scheduling gaps without blocking, smoothing throughput significantly.
    Beyond the saturation point (where producers never actually fill the
    buffer), further enlargement brings no additional throughput gains —
    throughput is then bounded by consumer processing speed alone.
 
    Key engineering insight: the optimal buffer size ≈ num_consumers ×
    avg_processing_time_per_item × producer_rate — a value large enough
    to absorb bursts without wasting memory on idle capacity.
""")
    print("=" * 72)
 
 
# ══════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════
 
def main() -> None:
    args = parse_args()
 
    logger.info("Producer-Consumer Experiment Suite  —  Task 2")
    logger.info(f"Total packets per experiment : {args.packets:,}")
    logger.info(f"Consumer processing delay    : {args.delay * 1000:.2f} ms/packet")
 
    collector = MetricsCollector()
 
    exp1_results = run_experiment1(args.packets, args.delay, collector)
    exp2_results = run_experiment2(args.packets, args.delay, collector)
 
    # Generate charts
    chart_path = str(Path(__file__).parent / "results.png")
    collector.generate_plots(exp1_results, exp2_results, chart_path)
 
    print_conclusions(exp1_results, exp2_results)
 
 
if __name__ == "__main__":
    main()
 