#!/usr/bin/env python3
"""Generate the ChopFlow desktop app icon set + menu bar glyphs.

Deterministic, dependency-light (Pillow only) regeneration of:

  - the macOS app icon: the whole icon (tile + background) at 85% of the
    canvas with squircle corners (0.2237x, the macOS Big Sur+ canonical
    rounding), a subtle near-black gradient tile (dark-app style), and the
    white Shepherd mark,
  - every size Tauri's `icon` config lists (32..1024 PNGs, .icns via
    `iconutil`, .ico),
  - the menu bar item glyphs: Google Material Symbols (play_arrow / stop /
    power_settings_new) rendered white-on-transparent for IconMenuItem.

Run from the repo root:  uv run --with pillow python scripts/gen-app-icons.py
"""

from __future__ import annotations

import math
import shutil
import subprocess
import tempfile
from pathlib import Path

from PIL import Image, ImageDraw, ImageOps

REPO = Path(__file__).resolve().parent.parent
ICONS = REPO / "app" / "src-auri" / "icons"

WHITE = (255, 255, 255, 255)
CORNER_RATIO = 0.2237  # macOS canonical squircle rounding
SS = 4  # supersampling factor for smooth edges
# The whole icon (background tile included) is drawn at 85% of the canvas so
# it does not read oversized next to other apps in the Dock; the tile is a
# subtle near-black vertical gradient (dark-app style), not a full-bleed fill.
ICON_SCALE = 0.85
GRAD_TOP = (42, 42, 48)  # #2A2A30
GRAD_BOT = (11, 11, 14)  # #0B0B0E

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

    # Tile: 85% of the canvas, centered, near-black vertical gradient masked
    # into the squircle.
    tile = int(big * ICON_SCALE)
    off = (big - tile) // 2
    grad = Image.new("RGBA", (tile, tile))
    gd = ImageDraw.Draw(grad)
    for y in range(tile):
        t = y / max(1, tile - 1)
        c = tuple(round(GRAD_TOP[i] + (GRAD_BOT[i] - GRAD_TOP[i]) * t) for i in range(3)) + (255,)
        gd.line([(0, y), (tile - 1, y)], fill=c)
    mask = Image.new("L", (tile, tile), 0)
    ImageDraw.Draw(mask).rounded_rectangle(
        (0, 0, tile - 1, tile - 1), radius=CORNER_RATIO * tile, fill=255
    )
    img.paste(grad, (off, off), mask)

    # White mark, same proportion of the tile as the first cut.
    d = ImageDraw.Draw(img)
    bx0, by0, bx1, by1 = mark_bbox()
    target_h = FIRST_CUT_MARK_HEIGHT * tile
    scale = target_h / (by1 - by0)
    cx = cy = big / 2
    mx = (bx0 + bx1) / 2
    my = (by0 + by1) / 2
    for path in MARK_PATHS:
        pts = [((x - mx) * scale + cx, (y - my) * scale + cy) for x, y in path]
        d.polygon(pts, fill=WHITE)

    return img.resize((size, size), Image.LANCZOS)


# ---------------------------------------------------------------------------
# Menu bar glyphs: Google Material Symbols (Rounded) rendered white-on-
# transparent at 16px for the macOS IconMenuItems. The source SVGs live in
# app/src-tauri/icons/material/ (Copyright Google LLC, Apache-2.0).
#
# macOS has no Python-callable system SVG rasterizer, so we render via
# `qlmanage -t` (which stamps a white page) and recover the glyph's alpha from
# the luminance: black glyph on white -> alpha = 255 - luminance.
# ---------------------------------------------------------------------------

MATERIAL_DIR = ICONS / "material"


def material_glyph(svg_name: str, size: int = 16) -> Image.Image:
    """Render icons/material/<svg_name> as a white-on-transparent icon."""
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(
            ["qlmanage", "-t", "-s", "512", "-o", tmp, str(MATERIAL_DIR / svg_name)],
            check=True,
            capture_output=True,
        )
        src = Image.open(Path(tmp) / f"{svg_name}.png").convert("L")
    alpha = ImageOps.invert(src)
    img = Image.new("RGBA", src.size, WHITE)
    img.putalpha(alpha)
    img = img.crop(img.getchannel("A").getbbox())
    side = max(img.size)
    canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    canvas.paste(img, ((side - img.width) // 2, (side - img.height) // 2), img)
    return canvas.resize((size, size), Image.LANCZOS)


def play_glyph(size: int = 16) -> Image.Image:
    return material_glyph("play_arrow.svg", size)


def stop_glyph(size: int = 16) -> Image.Image:
    return material_glyph("stop.svg", size)


def power_glyph(size: int = 16) -> Image.Image:
    return material_glyph("power_settings_new.svg", size)


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
