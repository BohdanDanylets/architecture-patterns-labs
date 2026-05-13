import multiprocessing as mp
import time
from pathlib import Path
from typing import List
 
from workers import (
    image_loader_worker,
    image_resizer_worker,
    image_processor_worker,
    image_saver_worker,
)
from metrics import PipelineMetrics
from logger import setup_logger
 
logger = setup_logger("Pipeline")
 
QUEUE_CAPACITY = 10
 
 
class ImagePipeline:
    """
    Constructs and runs the four-stage image processing pipeline.
 
    Args:
        input_dir:      Directory containing source images.
        output_dir:     Directory where processed images are written.
        target_size:    (width, height) for the resize stage.
        queue_capacity: Maximum items in each inter-stage queue (bounded buffer).
    """
 
    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        target_size: tuple = (1024, 1024),
        queue_capacity: int = QUEUE_CAPACITY,
    ):
        self.input_dir      = Path(input_dir)
        self.output_dir     = output_dir
        self.target_size    = target_size
        self.queue_capacity = queue_capacity
        self.metrics        = PipelineMetrics()
 
    def _collect_images(self) -> List[str]:
        """Scan input_dir for supported image formats."""
        extensions = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}
        paths = sorted(
            str(p)
            for p in self.input_dir.iterdir()
            if p.suffix.lower() in extensions
        )
        logger.info(f"Collected {len(paths)} images from '{self.input_dir}'")
        return paths
 
    def _drain_metrics(self, metrics_queue: mp.Queue, expected_done: int) -> None:
        """
        Runs in the main process.  Continuously reads from the shared
        metrics_queue until all 'done' signals have been received
        (one per stage).
 
        Why is this safe without locks?
          - Workers only PUT into metrics_queue.
          - The main process is the sole GETTER.
          - multiprocessing.Queue is internally serialised via a Pipe,
            so concurrent puts from multiple processes are safe.
        """
        stages_done = 0
 
        while stages_done < expected_done:
            msg = metrics_queue.get()  
 
            if "error" in msg:
                logger.error(
                    f"[{msg['stage']}] Error on '{msg.get('path', '?')}': {msg['error']}"
                )
                continue
 
            if msg.get("done"):
                stage_name = msg["stage"]
                self.metrics.get_stage(stage_name).mark_end()
                logger.info(f"[{stage_name}] Stage completed.")
                stages_done += 1
                continue
 
            sm = self.metrics.get_stage(msg["stage"])
            if "processing_time" in msg:
                sm.record_processing_time(msg["processing_time"])
            if "blocked_time" in msg:
                sm.record_blocked_time(msg["blocked_time"])
 
            if "output_path" in msg:
                logger.debug(f"[ImageSaver] ✓ Saved '{Path(msg['output_path']).name}'")
 
    def run(self) -> PipelineMetrics:
        """
        Start the pipeline and block until all images are processed.
 
        Returns:
            Populated PipelineMetrics with per-stage and overall timings.
        """
        image_paths = self._collect_images()
        if not image_paths:
            logger.warning("No images found — nothing to process.")
            return self.metrics
 
        q1 = mp.Queue(maxsize=self.queue_capacity)   
        q2 = mp.Queue(maxsize=self.queue_capacity)   
        q3 = mp.Queue(maxsize=self.queue_capacity)   
 
        mq = mp.Queue()
 
        processes = [
            mp.Process(
                target=image_loader_worker,
                args=(image_paths, q1, mq),
                name="ImageLoader",
                daemon=True,
            ),
            mp.Process(
                target=image_resizer_worker,
                args=(q1, q2, mq, self.target_size),
                name="ImageResizer",
                daemon=True,
            ),
            mp.Process(
                target=image_processor_worker,
                args=(q2, q3, mq),
                name="ImageProcessor",
                daemon=True,
            ),
            mp.Process(
                target=image_saver_worker,
                args=(q3, mq, self.output_dir),
                name="ImageSaver",
                daemon=True,
            ),
        ]
 
        logger.info(
            f"Launching pipeline | images={len(image_paths)} | "
            f"target={self.target_size} | queue_capacity={self.queue_capacity}"
        )
 
        self.metrics.mark_pipeline_start()
        for proc in processes:
            self.metrics.get_stage(proc.name).mark_start()
            proc.start()
            logger.info(f"  ✓ Started {proc.name}  (PID {proc.pid})")
 
        self._drain_metrics(mq, expected_done=len(processes))
 
        for proc in processes:
            proc.join(timeout=30)
            if proc.is_alive():
                logger.warning(f"Process {proc.name} timed out — terminating.")
                proc.terminate()
                proc.join(timeout=5)
 
        self.metrics.mark_pipeline_end()
 
        logger.info("Pipeline run complete.")
        self.metrics.print_report()
 
        return self.metrics
 