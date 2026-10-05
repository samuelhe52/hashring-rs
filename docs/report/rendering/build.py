#!/usr/bin/env python3
"""Build a Markdown report with shared Pandoc/CTeX styling."""

import argparse
import html
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from xml.etree import ElementTree


def run(command, **kwargs):
    return subprocess.run(command, check=True, **kwargs)


def rasterize(source, destination):
    """Rasterize at 3x SVG dimensions, preserving transparency and local fonts."""
    browser = os.environ.get("REPORT_BROWSER")
    if not browser:
        browser = next((shutil.which(name) for name in
                        ("chromium", "chromium-browser", "google-chrome")
                        if shutil.which(name)), None)
    mac_chrome = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    if not browser and mac_chrome.is_file():
        browser = str(mac_chrome)
    if browser:
        root = ElementTree.parse(source).getroot()
        _, _, width, height = map(float, root.attrib["viewBox"].split())
        page = destination.with_suffix(".html")
        page.write_text(
            '<!doctype html><meta charset="utf-8">'
            '<style>html,body{margin:0;padding:0;background:transparent}'
            'img{display:block}</style>'
            f'<img src="{html.escape(source.as_uri(), quote=True)}" '
            f'width="{round(width)}" height="{round(height)}">', encoding="utf-8")
        log = destination.with_suffix(".browser.log")
        destination.unlink(missing_ok=True)
        with tempfile.TemporaryDirectory(prefix=".browser-", dir=destination.parent) as profile, log.open("w") as stream:
            process = subprocess.Popen([browser, "--headless", "--disable-gpu", "--hide-scrollbars",
                 "--no-first-run", "--no-default-browser-check",
                 "--disable-background-networking", "--disable-component-update",
                 "--disable-sync", "--disable-crash-reporter",
                 "--allow-file-access-from-files", "--default-background-color=00000000",
                 "--force-device-scale-factor=3", "--virtual-time-budget=1000",
                 f"--window-size={round(width)},{round(height)}",
                 f"--user-data-dir={profile}",
                 f"--screenshot={destination}", page.as_uri()],
                stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if destination.is_file() and (
                            "bytes written to file" in log.read_text()
                            or process.poll() is not None):
                        break
                    if process.poll() is not None:
                        raise RuntimeError(f"SVG screenshot failed; inspect {log}")
                    time.sleep(0.1)
                else:
                    raise RuntimeError(f"SVG screenshot timed out; inspect {log}")
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
        if not destination.is_file():
            raise RuntimeError(f"SVG screenshot failed; inspect {log}")
    elif shutil.which("rsvg-convert"):
        run(["rsvg-convert", "--zoom", "3", "--output", str(destination), str(source)])
    else:
        raise RuntimeError("SVG figures require Chrome/Chromium or rsvg-convert")


def main():
    report_dir = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, nargs="?",
                        default=report_dir / "preliminary-report.md")
    parser.add_argument("--output", type=Path, help="Default: build/<report-name>.pdf")
    args = parser.parse_args()
    source = args.input.resolve()
    if not source.is_file():
        parser.error(f"Report does not exist: {source}")
    for dependency in ("pandoc", "xelatex"):
        if not shutil.which(dependency):
            parser.error(f"Required command not found: {dependency}")
    output = (args.output or report_dir / "build" / f"{source.stem}.pdf").resolve()
    work = report_dir / "build" / source.stem
    work.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    rendering = Path(__file__).resolve().parent

    result = run(["pandoc", str(source), "--from", "markdown", "--to", "json",
                  "--shift-heading-level-by=-1", "--metadata-file", str(rendering / "pdf.yaml")],
                 capture_output=True, text=True, cwd=source.parent)
    document = json.loads(result.stdout)
    images = {}

    def prepare(node):
        if isinstance(node, dict):
            if node.get("t") == "Image":
                target = node["c"][2][0]
                if "://" in target:
                    raise RuntimeError("Report images must be local files")
                image = (source.parent / target).resolve()
                if not image.is_file():
                    raise RuntimeError(f"Image does not exist: {image}")
                if image.suffix.lower() == ".svg":
                    if image not in images:
                        destination = work / f"figure-{len(images) + 1}.png"
                        rasterize(image, destination)
                        images[image] = destination
                    image = images[image]
                node["c"][2][0] = str(image)
            for child in node.values():
                prepare(child)
        elif isinstance(node, list):
            for child in node:
                prepare(child)

    prepare(document)
    ast = work / "report.json"
    ast.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
    tex = work / "report.tex"
    run(["pandoc", str(ast), "--from", "json", "--to", "latex", "--standalone", "--citeproc",
         "--lua-filter", str(rendering / "layout.lua"),
         "--include-in-header", str(rendering / "header.tex"), "--output", str(tex)],
        cwd=source.parent)
    for pass_number in (1, 2):
        with (work / f"xelatex-{pass_number}.log").open("w") as log:
            try:
                run(["xelatex", "-interaction=nonstopmode", "-halt-on-error", tex.name],
                    cwd=work, stdout=log, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError:
                raise RuntimeError(f"XeLaTeX failed; inspect {log.name}") from None
    shutil.copyfile(work / "report.pdf", output)
    print(output)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Build failed: {error}", file=sys.stderr)
        sys.exit(1)
