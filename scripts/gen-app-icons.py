#!/usr/bin/env python3
"""Generate the ChopFlow desktop app icon set + menu bar glyphs.

Deterministic, dependency-light (Pillow only) regeneration of:

  - the macOS app icon: an indigo (#605DFF) squircle tile (corner radius
    0.2237x, the macOS Big Sur+ canonical rounding) carrying the white
    Shepherd mark at a restrained scale (85% of the first cut, which filled
    ~90% of the tile and read oversized in the Dock),
  - every size Tauri's `icon` config lists (32..1024 PNGs, .icns via
    `iconutil`, .ico),
  - the menu bar item glyphs (play / stop / power) as SF-Symbols-style
    white-on-transparent icons for IconMenuItem.

Run from the repo root:  uv run --with pillow python scripts/gen-app-icons.py
"""

from __future__ import annotations

import math
import shutil
import subprocess
import tempfile
from pathlib import Path

from PIL import Image, ImageDraw

REPO = Path(__file__).resolve().parent.parent
ICONS = REPO / "app" / "src-auri" / "icons"

TILE = (96, 93, 255, 255)  # #605DFF indigo
WHITE = (255, 255, 255, 255)
CORNER_RATIO = 0.2237  # macOS canonical squircle rounding
MARK_SCALE = 0.85  # vs. the first cut (mark height ~89.6% of the tile)
SS = 4  # supersampling factor for smooth edges

# The canonical ChopFlow Shepherd mark (assets/icons/chopflow.svg), white-only
# polygon paths — the same geometry Welcome.tsx renders inside the brand tile.
MARK_PATHS = [
    [(617, 234), (513, 390), (461, 506), (329, 685), (310, 762), (355, 686),
     (491, 542), (517, 577), (548, 522), (537, 472), (609, 285), (641, 434),
     (612, 492), (674, 443)],
    [(780, 278), (696, 402), (723, 420), (768, 334), (768, 386), (751, 436),
     (780, 459)],
    [(695, 430), (623, 579), (517, 655), (493, 763), (614, 893), (648, 980),
     (670, 825), (603, 743), (767, 617), (757, 595), (680, 568), (749, 539),
     (739, 509), (781, 535), (799, 599), (923, 669), (907, 690), (943, 732),
     (976, 685), (834, 584), (816, 521)],
    [(918, 757), (882, 782), (854, 788), (838, 788), (740, 760), (715, 771),
     (745, 776), (842, 805), (856, 805), (888, 797)],
]

# First-cut mark height as a fraction of the tile (measured from the shipped
# 1024px icon: 918/1024). The new mark is 85% of that.
FIRST_CUT_MARK_HEIGHT = 918 / 1024


def mark_bbox() -> tuple[float, float, float, float]:
    xs = [p[0] for path in MARK_PATHS for p in path]
    ys = [p[1] for path in MARK_PATHS for p in path]
    return min(xs), min(ys), max(xs), max(ys)


def app_icon(size: int) -> Image.Image:
    """Squircle tile + white Shepherd mark, supersampled then downscaled."""
    big = size * SS
    img = Image.new("RGBA", (big, big), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)

    radius = CORNER_RATIO * big
    d.rounded_rectangle((0, 0, big - 1, big - 1), radius=radius, fill=TILE)

    # Scale the mark so its content height is MARK_SCALE of the first cut,
    # centered on the tile.
    bx0, by0, bx1, by1 = mark_bbox()
    native_h = by1 - by0
    target_h = FIRST_CUT_MARK_HEIGHT * MARK_SCALE * big
    scale = target_h / native_h
    cx = cy = big / 2
    mx = (bx0 + bx1) / 2
    my = (by0 + by1) / 2
    for path in MARK_PATHS:
        pts = [((x - mx) * scale + cx, (y - my) * scale + cy) for x, y in path]
        d.polygon(pts, fill=WHITE)

    return img.resize((size, size), Image.LANCZOS)


# ---------------------------------------------------------------------------
# Menu bar glyphs: SF-Symbols-style white-on-transparent, 16pt menu icons.
# Drawn on an alpha mask (mode L) at 32x so strokes can be erased (the power
# symbol's gap) before becoming the white image's alpha.
# ---------------------------------------------------------------------------

def _glyph(size: int, painter) -> Image.Image:
    big = size * 32
    mask = Image.new("L", (big, big), 0)
    painter(ImageDraw.Draw(mask), big)
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    img.putalpha(mask.resize((size, size), Image.LANCZOS))
    # white everywhere the mask allows
    full = Image.new("RGBA", (size, size), WHITE)
    return Image.composite(full, img, img.getchannel("A"))


def play_glyph(size: int = 16) -> Image.Image:
    """SF `play.fill`: rounded triangle pointing right."""

    def paint(d: ImageDraw.ImageDraw, s: int):
        r = s * 0.075  # corner rounding
        a, b, c = (s * 0.34, s * 0.22), (s * 0.34, s * 0.78), (s * 0.78, s * 0.5)
        d.polygon([a, b, c], fill=255)
        for p in (a, b, c):
            d.ellipse((p[0] - r, p[1] - r, p[0] + r, p[1] + r), fill=255)

    return _glyph(size, paint)


def stop_glyph(size: int = 16) -> Image.Image:
    """SF `stop.fill`: rounded square."""

    def paint(d: ImageDraw.ImageDraw, s: int):
        m = s * 0.22
        d.rounded_rectangle((m, m, s - m, s - m), radius=s * 0.16, fill=255)

    return _glyph(size, paint)


def power_glyph(size: int = 16) -> Image.Image:
    """SF `power`: open circle with a vertical stem at the top."""

    def paint(d: ImageDraw.ImageDraw, s: int):
        w = max(2, round(s * 0.11))
        cx = s / 2
        cy = s * 0.56
        rr = s * 0.30
        d.ellipse((cx - rr, cy - rr, cx + rr, cy + rr), outline=255, width=w)
        # Open the circle where the stem enters (top), then draw the stem.
        d.pieslice(
            (cx - rr - w, cy - rr - w, cx + rr + w, cy + rr + w),
            start=250,
            end=290,
            fill=0,
        )
        x0, y0, y1 = cx, s * 0.12, cy - rr * 0.4
        d.line((x0, y0, x0, y1), fill=255, width=w)
        for y in (y0, y1):
            d.ellipse((x0 - w / 2, y - w / 2, x0 + w / 2, y + w / 2), fill=255)

    return _glyph(size, paint)


def build_icns(master: Image.Image) -> None:
    """Assemble the .icns from an iconset directory via iconutil (macOS)."""
    with tempfile.TemporaryDirectory() as tmp:
        iconset = Path(tmp) / "icon.iconset"
        iconset.mkdir()
        sizes = {16: 1, 32: 1, 128: 1, 256: 1, 512: 1}
        for base, mult in sizes.items():
            for factor, name in ((1, f"icon_{base}x{base}.png"),
                                 (2, f"icon_{base}x{base}@2x.png")):
                px = base * mult * factor
                master.resize((px, px), Image.LANCZOS).save(iconset / name)
        subprocess.run(
            ["iconutil", "-c", "icns", str(iconset), "-o", str(ICONS / "icon.icns")],
            check=True,
        )


def main() -> None:
    master = app_icon(1024)
    for px in (32, 64, 128, 256, 512, 1024):
        master.resize((px, px), Image.LANCZOS).save(ICONS / f"{px}x{px}.png")
    master.resize((256, 256), Image.LANCZOS).save(ICONS / "128x128@2x.png")
    master.save(ICONS / "icon.png")
    build_icns(master)

    # Windows .ico from the same master.
    master.resize((256, 256), Image.LANCZOS).save(
        ICONS / "icon.ico",
        sizes=[(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)],
    )

    # Menu bar glyphs (white, transparent background).
    play_glyph().save(ICONS / "menu-play.png")
    stop_glyph().save(ICONS / "menu-stop.png")
    power_glyph().save(ICONS / "menu-power.png")

    print(f"icons regenerated in {ICONS}")


if __name__ == "__main__":
    if not shutil.which("iconutil"):
        raise SystemExit("iconutil not found — run on macOS")
    main()
