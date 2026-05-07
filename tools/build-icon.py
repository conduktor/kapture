#!/usr/bin/env python3
"""Apply a macOS-template squircle alpha mask to icon-source.png.

macOS does not auto-mask icons in the Dock — the .icns ships
verbatim. A source PNG without alpha (like our prior `icon-source.png`,
RGB only) renders as a coloured square. This script alpha-masks the
source to the canonical Big Sur rounded-square shape so the Dock
silhouette matches every other native app.

Radius 228 px on a 1024 px canvas (~22.3 %) is the de-facto template
most macOS apps use. It's a circular-arc rounded rectangle, not
Apple's true continuous-corner superellipse, but the visual delta is
imperceptible at Dock sizes.

Usage:
    python tools/build-icon.py              # in-place on icon-source.png
    python tools/build-icon.py in.png out.png
"""
from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image, ImageDraw

CANVAS = 1024
RADIUS = 228


def shape(inp: Path, out: Path) -> None:
    src = Image.open(inp).convert("RGBA")
    if src.size != (CANVAS, CANVAS):
        src = src.resize((CANVAS, CANVAS), Image.Resampling.LANCZOS)

    mask = Image.new("L", (CANVAS, CANVAS), 0)
    ImageDraw.Draw(mask).rounded_rectangle(
        (0, 0, CANVAS, CANVAS), radius=RADIUS, fill=255
    )

    shaped = Image.new("RGBA", (CANVAS, CANVAS), (0, 0, 0, 0))
    shaped.paste(src, (0, 0), mask)
    shaped.save(out, "PNG")
    print(f"wrote {out} (RGBA {CANVAS}×{CANVAS}, squircle r={RADIUS})")


def main() -> int:
    args = sys.argv[1:]
    if len(args) == 0:
        inp = out = Path("icon-source.png")
    elif len(args) == 1:
        inp = out = Path(args[0])
    elif len(args) == 2:
        inp, out = Path(args[0]), Path(args[1])
    else:
        print(__doc__, file=sys.stderr)
        return 2
    if not inp.exists():
        print(f"input not found: {inp}", file=sys.stderr)
        return 1
    shape(inp, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
