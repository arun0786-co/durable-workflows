"""Run a few real commands in a scratch directory and save their output as a PNG.

usage: uv run --with playwright==1.58.0 --with pillow python scripts/capture_terminal.py
writes docs/images/orders_history.png and docs/images/sim_run.png

The commands run against this checkout (PYTHONPATH points at the repo root) in a
temporary directory, so orders.db is not left in the repo. The text in the
images is exactly what the commands printed.
"""

import html
import io
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "images"

SHOTS = {
    "orders_history.png": [
        "python examples/orders.py",
        "python -m durable show orders.db order-2",
    ],
    "sim_run.png": [
        "python -m durable.sim --seeds 200",
        "python -m durable.sim --seeds 100 --mutation split_commit",
    ],
}

PAGE = """<!doctype html><meta charset="utf-8"><style>
body {{ margin: 0; background: #ffffff; }}
pre {{ margin: 0; padding: 18px 22px; font: 13px/1.45 Menlo, "SF Mono", monospace; color: #1d1d1f;
       white-space: pre; display: inline-block; }}
.cmd {{ color: #1f4e79; font-weight: 600; }}
</style><pre>{body}</pre>"""


def run(cmds, cwd):
    env = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONUNBUFFERED="1")
    parts = []
    for cmd in cmds:
        argv = cmd.split()
        argv[0] = sys.executable
        argv = [str(ROOT / a) if a.startswith("examples/") else a for a in argv]
        out = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True, timeout=600)
        parts.append(f'<span class="cmd">$ {html.escape(cmd)}</span>\n{html.escape(out.stdout + out.stderr)}')
    return "\n".join(parts).rstrip() + "\n"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp, sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
        for name, cmds in SHOTS.items():
            page.set_content(PAGE.format(body=run(cmds, tmp)))
            png = page.locator("pre").screenshot()
            im = Image.open(io.BytesIO(png)).convert("RGB")
            if im.width > 1800:
                im = im.resize((1800, round(im.height * 1800 / im.width)), Image.LANCZOS)
            im.quantize(colors=64, method=Image.Quantize.FASTOCTREE, dither=Image.Dither.NONE).save(OUT / name, optimize=True)
            print(f"wrote docs/images/{name}  {(OUT / name).stat().st_size // 1024} KB")
        browser.close()


if __name__ == "__main__":
    main()
