import time
from pathlib import Path
 
import multiprocessing as mp
from PIL import Image, ImageFilter
import numpy as np
 
# ---------------------------------------------------------------------------
# Sentinel value — the Poison Pill that signals end-of-stream
# ---------------------------------------------------------------------------
POISON_PILL = None
 
 
# ===========================================================================
#  STAGE 1 — ImageLoader (Source)
# ===========================================================================
 
def image_loader_worker(
    image_paths: list,
    out_queue: mp.Queue,
    metrics_queue: mp.Queue,
) -> None:
    """
    Reads image files from disk and places them into out_queue.
 
    Synchronization:
      out_queue.put() blocks if the Resizer stage is slower than the
      Loader — preventing unbounded memory growth (backpressure).
 
    After exhausting all paths, a POISON_PILL is placed to propagate
    the shutdown signal downstream.
    """
    stage_name = "ImageLoader"
 
    for path in image_paths:
        try:
            t_start = time.perf_counter()
 
            # Open and immediately copy the image so the file handle is
            # released.  copy() is required because PIL opens files lazily.
            img = Image.open(path).copy()
 
            processing_elapsed = time.perf_counter() - t_start
 
            # Measure how long put() blocks (= time waiting on full queue)
            t_put = time.perf_counter()
            out_queue.put((str(path), img))         # Blocks if queue is full
            blocked = max(0.0, time.perf_counter() - t_put - processing_elapsed)
 
            metrics_queue.put({
                "stage":           stage_name,
                "processing_time": processing_elapsed,
                "blocked_time":    blocked,
            })
 
        except Exception as exc:
            metrics_queue.put({
                "stage": stage_name,
                "error": str(exc),
                "path":  str(path),
            })
 
    # Signal: no more images
    out_queue.put(POISON_PILL)
    metrics_queue.put({"stage": stage_name, "done": True})
 
 
# ===========================================================================
#  STAGE 2 — ImageResizer (Filter 1)
# ===========================================================================
 
def image_resizer_worker(
    in_queue: mp.Queue,
    out_queue: mp.Queue,
    metrics_queue: mp.Queue,
    target_size: tuple = (1024, 1024),
) -> None:
    """
    Resizes each image to target_size using high-quality Lanczos resampling.
 
    Lanczos is chosen over BILINEAR or NEAREST because it produces the
    sharpest result and is realistic workload for a vision preprocessing
    pipeline.
 
    Synchronization:
      in_queue.get()  — blocks if Loader has not produced yet (empty queue).
      out_queue.put() — blocks if Processor is slower (full queue / backpressure).
    """
    stage_name = "ImageResizer"
 
    while True:
        item = in_queue.get()            # Block until Loader produces an item
 
        if item is POISON_PILL:
            out_queue.put(POISON_PILL)   # Propagate shutdown to Processor
            metrics_queue.put({"stage": stage_name, "done": True})
            break
 
        path, img = item
 
        try:
            t_start = time.perf_counter()
 
            # Ensure consistent colour depth before resize
            img_rgb = img.convert("RGB")
            resized  = img_rgb.resize(target_size, Image.Resampling.LANCZOS)
 
            proc_elapsed = time.perf_counter() - t_start
 
            t_put = time.perf_counter()
            out_queue.put((path, resized))
            blocked = max(0.0, time.perf_counter() - t_put - proc_elapsed)
 
            metrics_queue.put({
                "stage":           stage_name,
                "processing_time": proc_elapsed,
                "blocked_time":    blocked,
            })
 
        except Exception as exc:
            metrics_queue.put({"stage": stage_name, "error": str(exc), "path": path})
 
 
# ===========================================================================
#  STAGE 3 — ImageProcessor (Filter 2) — CPU-heavy
# ===========================================================================
 
def image_processor_worker(
    in_queue: mp.Queue,
    out_queue: mp.Queue,
    metrics_queue: mp.Queue,
) -> None:
    """
    Applies a computationally intensive processing chain:
 
      1. Convert to grayscale (luminosity model)
      2. Gaussian blur  — noise suppression (radius=2)
      3. FIND_EDGES     — Sobel-based edge detection kernel
      4. UnsharpMask    — sharpening pass (radius=3, 200 %)
      5. Numpy normalisation — histogram stretch (min-max scaling)
      6. Convert back to RGB for downstream JPEG compatibility
 
    This stage simulates the kind of preprocessing applied to chess-board
    images before feeding them into a CNN (contour extraction + normalisation).
 
    This is intentionally the heaviest stage and is expected to be the
    pipeline bottleneck, which the metrics report will confirm.
    """
    stage_name = "ImageProcessor"
 
    while True:
        item = in_queue.get()
 
        if item is POISON_PILL:
            out_queue.put(POISON_PILL)
            metrics_queue.put({"stage": stage_name, "done": True})
            break
 
        path, img = item
 
        try:
            t_start = time.perf_counter()
 
            # --- Step 1: Normalise input ---
            rgb = img.convert("RGB")
 
            # --- Step 2: Grayscale ---
            gray = rgb.convert("L")
 
            # --- Step 3: Gaussian blur (noise suppression) ---
            blurred = gray.filter(ImageFilter.GaussianBlur(radius=2))
 
            # --- Step 4: Edge detection (Sobel-like built-in kernel) ---
            edges = blurred.filter(ImageFilter.FIND_EDGES)
 
            # --- Step 5: Sharpening via Unsharp Mask ---
            sharpened = edges.filter(
                ImageFilter.UnsharpMask(radius=3, percent=200, threshold=3)
            )
 
            # --- Step 6: Numpy min-max normalisation (histogram stretch) ---
            arr = np.array(sharpened, dtype=np.float32)
            arr_min, arr_max = arr.min(), arr.max()
            arr_norm = (arr - arr_min) / (arr_max - arr_min + 1e-8) * 255.0
            normalised = Image.fromarray(arr_norm.astype(np.uint8), mode="L")
 
            # --- Step 7: Back to RGB for JPEG output ---
            result = normalised.convert("RGB")
 
            proc_elapsed = time.perf_counter() - t_start
 
            t_put = time.perf_counter()
            out_queue.put((path, result))
            blocked = max(0.0, time.perf_counter() - t_put - proc_elapsed)
 
            metrics_queue.put({
                "stage":           stage_name,
                "processing_time": proc_elapsed,
                "blocked_time":    blocked,
            })
 
        except Exception as exc:
            metrics_queue.put({"stage": stage_name, "error": str(exc), "path": path})
 
 
# ===========================================================================
#  STAGE 4 — ImageSaver (Sink)
# ===========================================================================
 
def image_saver_worker(
    in_queue: mp.Queue,
    metrics_queue: mp.Queue,
    output_dir: str,
) -> None:
    """
    Writes processed images to the output directory.
 
    Naming convention: <original_stem>_processed.jpg
 
    This stage is I/O-bound (disk write) and can become a secondary
    bottleneck on slow storage.  The metrics will show the average
    save time per image.
 
    Synchronization:
      in_queue.get() blocks until the Processor delivers a result.
      No out_queue because this is the terminal sink.
    """
    stage_name = "ImageSaver"
    out_path   = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
 
    while True:
        item = in_queue.get()
 
        if item is POISON_PILL:
            metrics_queue.put({"stage": stage_name, "done": True})
            break
 
        path, img = item
 
        try:
            t_start = time.perf_counter()
 
            stem         = Path(path).stem
            output_file  = out_path / f"{stem}_processed.jpg"
            img.save(str(output_file), format="JPEG", quality=90, optimize=True)
 
            proc_elapsed = time.perf_counter() - t_start
 
            metrics_queue.put({
                "stage":           stage_name,
                "processing_time": proc_elapsed,
                "blocked_time":    0.0,
                "output_path":     str(output_file),
            })
 
        except Exception as exc:
            metrics_queue.put({"stage": stage_name, "error": str(exc), "path": path})
 