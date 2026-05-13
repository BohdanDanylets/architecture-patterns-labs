import argparse
import sys
from pathlib import Path
 
# Allow importing sibling modules without installing the package
sys.path.insert(0, str(Path(__file__).parent))
 
from pipeline import ImagePipeline
from utils import generate_test_images
from logger import setup_logger
 
logger = setup_logger("Main")
 
# ---------------------------------------------------------------------------
# Paths relative to this script's directory
# ---------------------------------------------------------------------------
INPUT_DIR  = str(Path(__file__).parent / "input")
OUTPUT_DIR = str(Path(__file__).parent / "output")
 
 
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Multithreaded Image Processing Pipeline (Task 1 — Pipeline Pattern)"
    )
    p.add_argument(
        "--images",
        type=int,
        default=50,
        help="Number of synthetic test images to generate if input/ is empty (default: 50)",
    )
    p.add_argument(
        "--size",
        type=str,
        default="1024x1024",
        help="Target resize dimension WxH (default: 1024x1024)",
    )
    p.add_argument(
        "--queue-size",
        type=int,
        default=10,
        help="Max items per inter-stage queue (default: 10)",
    )
    return p.parse_args()
 
 
def main() -> None:
    args = parse_args()
 
    # Parse target size
    try:
        w, h = map(int, args.size.lower().split("x"))
        target_size = (w, h)
    except ValueError:
        logger.error(f"Invalid --size format '{args.size}'. Expected WxH, e.g. 1024x1024")
        sys.exit(1)
 
    logger.info("=" * 62)
    logger.info("  IMAGE PROCESSING PIPELINE  —  TASK 1  (Pipeline Pattern)")
    logger.info("=" * 62)
    logger.info(f"  Input  dir   : {INPUT_DIR}")
    logger.info(f"  Output dir   : {OUTPUT_DIR}")
    logger.info(f"  Target size  : {target_size[0]}×{target_size[1]}")
    logger.info(f"  Queue cap    : {args.queue_size}")
    logger.info("=" * 62)
 
    # Populate the input directory with synthetic images if needed
    generate_test_images(INPUT_DIR, count=args.images, size=(512, 512))
 
    # Build and execute the pipeline
    pipeline = ImagePipeline(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        target_size=target_size,
        queue_capacity=args.queue_size,
    )
 
    metrics = pipeline.run()
 
    logger.info(
        f"All done. Total wall-clock time: {metrics.total_pipeline_time:.3f}s | "
        f"Results → {OUTPUT_DIR}"
    )
 
 
if __name__ == "__main__":
    # freeze_support() is required on Windows when using multiprocessing
    # with a frozen executable (e.g. PyInstaller).  No-op on Linux/macOS.
    import multiprocessing
    multiprocessing.freeze_support()
    main()
 