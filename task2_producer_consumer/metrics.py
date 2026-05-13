from dataclasses import dataclass
from pathlib import Path
from typing import List
 
try:
    import matplotlib
    matplotlib.use("Agg")           # Non-interactive backend — works on servers
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    _MPL = True
except ImportError:
    _MPL = False
 
 
@dataclass
class ExperimentResult:
    """All measured metrics for one Producer-Consumer experiment run."""
    label:                   str
    num_producers:           int
    num_consumers:           int
    buffer_size:             int
    total_packets:           int    # actually consumed
    elapsed_time:            float  # wall-clock seconds
    throughput:              float  # packets / second
    producer_blocks:         int    # times producers waited on full queue
    consumer_blocks:         int    # times consumers waited on empty queue
    avg_producer_throughput: float  # avg pkt/s across producer threads
    avg_consumer_throughput: float  # avg pkt/s across consumer threads
 
 
class MetricsCollector:
    """Aggregates ExperimentResult objects and produces formatted output."""
 
    def __init__(self) -> None:
        self.results: List[ExperimentResult] = []
 
    def record(self, result: ExperimentResult) -> None:
        self.results.append(result)
 
    # ------------------------------------------------------------------
    # Table printers
    # ------------------------------------------------------------------
 
    @staticmethod
    def print_experiment1_table(results: List[ExperimentResult]) -> None:
        """Print Experiment 1 results: varying P/C ratio."""
        SEP = "=" * 90
        print(f"\n{SEP}")
        print("  EXPERIMENT 1 — Producer / Consumer Ratio vs Throughput  (buffer=100, fixed)")
        print(SEP)
        hdr = (
            f"  {'Configuration':<22} {'Time (s)':>10} "
            f"{'Throughput (pkt/s)':>20} {'Prod Blocks':>13} {'Cons Blocks':>13}"
        )
        print(hdr)
        print(f"  {'─'*22} {'─'*10} {'─'*20} {'─'*13} {'─'*13}")
        for r in results:
            config = f"{r.num_producers}P  /  {r.num_consumers}C"
            print(
                f"  {config:<22} {r.elapsed_time:>10.3f} "
                f"{r.throughput:>20.1f} {r.producer_blocks:>13} {r.consumer_blocks:>13}"
            )
        print(f"{SEP}\n")
 
    @staticmethod
    def print_experiment2_table(results: List[ExperimentResult]) -> None:
        """Print Experiment 2 results: varying buffer size."""
        SEP = "=" * 90
        print(f"\n{SEP}")
        print("  EXPERIMENT 2 — Buffer Size Impact  (5P / 5C, fixed)")
        print(SEP)
        hdr = (
            f"  {'Buffer Size':>14} {'Time (s)':>10} "
            f"{'Throughput (pkt/s)':>20} {'Prod Blocks':>13} {'Cons Blocks':>13}"
        )
        print(hdr)
        print(f"  {'─'*14} {'─'*10} {'─'*20} {'─'*13} {'─'*13}")
        for r in results:
            print(
                f"  {r.buffer_size:>14} {r.elapsed_time:>10.3f} "
                f"{r.throughput:>20.1f} {r.producer_blocks:>13} {r.consumer_blocks:>13}"
            )
        print(f"{SEP}\n")
 
    # ------------------------------------------------------------------
    # Visualisation
    # ------------------------------------------------------------------
 
    @staticmethod
    def generate_plots(
        exp1_results: List[ExperimentResult],
        exp2_results: List[ExperimentResult],
        output_path: str = "results.png",
    ) -> None:
        """
        Produce a 2×3 matplotlib figure and save it as a PNG.
 
        Row 0 — Experiment 1 (bar charts)
        Row 1 — Experiment 2 (line charts with log-scaled x-axis)
        """
        if not _MPL:
            print("[Metrics] matplotlib not installed — skipping plot generation.")
            return
 
        fig = plt.figure(figsize=(18, 11))
        fig.suptitle(
            "Producer-Consumer Performance Analysis",
            fontsize=17,
            fontweight="bold",
            y=0.98,
        )
        gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.50, wspace=0.38)
 
        # ── Colour palette ──────────────────────────────────────────
        BLUE   = "#2196F3"
        GREEN  = "#4CAF50"
        RED    = "#F44336"
        ORANGE = "#FF9800"
        PURPLE = "#9C27B0"
        TEAL   = "#009688"
 
        # ── Experiment 1 data ───────────────────────────────────────
        configs      = [f"{r.num_producers}P/{r.num_consumers}C" for r in exp1_results]
        throughput1  = [r.throughput       for r in exp1_results]
        times1       = [r.elapsed_time     for r in exp1_results]
        p_blocks1    = [r.producer_blocks  for r in exp1_results]
        c_blocks1    = [r.consumer_blocks  for r in exp1_results]
        colors1      = [BLUE, GREEN, RED]
 
        # — E1 subplot 1: Throughput —
        ax0 = fig.add_subplot(gs[0, 0])
        bars = ax0.bar(configs, throughput1, color=colors1, edgecolor="white", linewidth=0.8)
        ax0.set_title("Exp 1 — Throughput by Config", fontweight="bold")
        ax0.set_ylabel("Packets / second")
        ax0.set_xlabel("Configuration (Producers / Consumers)")
        for bar, val in zip(bars, throughput1):
            ax0.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() * 1.01,
                f"{val:,.0f}",
                ha="center", va="bottom", fontsize=9,
            )
        ax0.set_ylim(0, max(throughput1) * 1.20)
 
        # — E1 subplot 2: Execution time —
        ax1 = fig.add_subplot(gs[0, 1])
        ax1.bar(configs, times1, color=colors1, edgecolor="white", linewidth=0.8)
        ax1.set_title("Exp 1 — Execution Time by Config", fontweight="bold")
        ax1.set_ylabel("Time (seconds)")
        ax1.set_xlabel("Configuration")
 
        # — E1 subplot 3: Blocking events —
        ax2 = fig.add_subplot(gs[0, 2])
        x     = range(len(configs))
        width = 0.35
        ax2.bar(
            [i - width / 2 for i in x], p_blocks1, width,
            label="Producer Blocks", color=ORANGE, edgecolor="white",
        )
        ax2.bar(
            [i + width / 2 for i in x], c_blocks1, width,
            label="Consumer Blocks", color=PURPLE, edgecolor="white",
        )
        ax2.set_title("Exp 1 — Blocking Events by Config", fontweight="bold")
        ax2.set_ylabel("Block Count")
        ax2.set_xticks(list(x))
        ax2.set_xticklabels(configs)
        ax2.legend(fontsize=8)
 
        # ── Experiment 2 data ───────────────────────────────────────
        buf_sizes    = [r.buffer_size      for r in exp2_results]
        throughput2  = [r.throughput       for r in exp2_results]
        p_blocks2    = [r.producer_blocks  for r in exp2_results]
        c_blocks2    = [r.consumer_blocks  for r in exp2_results]
 
        def _log_line_chart(ax, x, y, color, title, ylabel):
            ax.plot(x, y, "o-", color=color, linewidth=2.2, markersize=9, zorder=3)
            for xi, yi in zip(x, y):
                ax.annotate(
                    f"{yi:,.0f}", (xi, yi),
                    textcoords="offset points", xytext=(0, 8),
                    ha="center", fontsize=8,
                )
            ax.set_title(title, fontweight="bold")
            ax.set_ylabel(ylabel)
            ax.set_xlabel("Buffer Size (log scale)")
            ax.set_xscale("log")
            ax.set_xticks(buf_sizes)
            ax.set_xticklabels([str(b) for b in buf_sizes])
            ax.grid(True, which="both", alpha=0.25)
 
        # — E2 subplot 1: Throughput —
        _log_line_chart(
            fig.add_subplot(gs[1, 0]),
            buf_sizes, throughput2, TEAL,
            "Exp 2 — Throughput vs Buffer Size",
            "Packets / second",
        )
 
        # — E2 subplot 2: Producer blocks —
        _log_line_chart(
            fig.add_subplot(gs[1, 1]),
            buf_sizes, p_blocks2, ORANGE,
            "Exp 2 — Producer Blocks vs Buffer Size",
            "Block Count",
        )
 
        # — E2 subplot 3: Consumer blocks —
        _log_line_chart(
            fig.add_subplot(gs[1, 2]),
            buf_sizes, c_blocks2, PURPLE,
            "Exp 2 — Consumer Blocks vs Buffer Size",
            "Block Count",
        )
 
        # ── Save ────────────────────────────────────────────────────
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"\n[Metrics] Chart saved → {output_path}")
        plt.close(fig)
 