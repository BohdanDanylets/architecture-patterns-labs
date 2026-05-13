import random
from pathlib import Path
 
from PIL import Image, ImageDraw
import numpy as np
 
 
def generate_test_images(
    output_dir: str,
    count: int = 50,
    size: tuple = (512, 512),
) -> int:
    """
    Generate synthetic chess-board–style images for pipeline testing.
 
    Each image contains:
      - An 8×8 alternating dark/light grid (chessboard pattern).
      - Random coloured noise overlaid on each cell.
      - 4-16 random circles simulating chess piece positions.
 
    The images are intentionally complex enough to make the CPU-heavy
    ImageProcessor stage non-trivial.
 
    Args:
        output_dir: Directory where images are written.
        count:      Number of images to generate.
        size:       (width, height) in pixels.
 
    Returns:
        Number of images available in output_dir after the call.
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
 
    # If images already exist, skip regeneration to save time during reruns.
    existing = list(out_path.glob("*.jpg")) + list(out_path.glob("*.png"))
    if existing:
        print(f"[utils] Found {len(existing)} existing images — skipping generation.")
        return len(existing)
 
    print(f"[utils] Generating {count} synthetic chess-board images ({size[0]}×{size[1]}) ...")
 
    light_colors = [
        (240, 217, 181),   # classic chess light square
        (255, 255, 204),
        (210, 210, 190),
    ]
    dark_colors = [
        (181, 136, 99),    # classic chess dark square
        (100, 80, 60),
        (60, 60, 40),
    ]
 
    cell_size = size[0] // 8
 
    for i in range(count):
        # Pick random board colours for variety
        light = random.choice(light_colors)
        dark  = random.choice(dark_colors)
 
        # Start with low-level noise so textures are not flat
        noise = np.random.randint(0, 20, (*size, 3), dtype=np.uint8)
        img   = Image.fromarray(noise, mode="RGB")
        draw  = ImageDraw.Draw(img)
 
        # Draw an 8×8 chessboard grid
        for row in range(8):
            for col in range(8):
                x0 = col * cell_size
                y0 = row * cell_size
                x1 = x0 + cell_size
                y1 = y0 + cell_size
                color = light if (row + col) % 2 == 0 else dark
                draw.rectangle([x0, y0, x1, y1], fill=color)
 
        # Draw random circles simulating piece positions
        num_pieces = random.randint(4, 16)
        for _ in range(num_pieces):
            col = random.randint(0, 7)
            row = random.randint(0, 7)
            cx  = col * cell_size + cell_size // 2
            cy  = row * cell_size + cell_size // 2
            r   = cell_size // 3
            piece_color = (
                random.choice([(240, 240, 240), (30, 30, 30)])
            )
            draw.ellipse(
                [cx - r, cy - r, cx + r, cy + r],
                fill=piece_color,
                outline=(0, 0, 0),
                width=2,
            )
 
        file_path = out_path / f"chess_{i:05d}.jpg"
        img.save(str(file_path), format="JPEG", quality=85)
 
    print(f"[utils] Done — {count} images written to '{output_dir}'.")
    return count

if __name__ == "__main__":
    from pathlib import Path

    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    generate_test_images(
        output_dir=input_dir,
        count=100
    )

    print("Generated 100 test images in input/")