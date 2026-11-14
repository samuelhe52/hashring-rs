# Reports

[final-report.md](final-report.md) is the final report.
[preliminary-report.md](preliminary-report.md) is the preliminary technical report.
[related-work.md](related-work.md) contains supporting material.

[preliminary-slides.html](preliminary-slides.html) is the self-contained
defense deck (Chinese, about 6–7 minutes). Open it in a browser: arrow keys
change slides, `N` shows speaker notes, `F` toggles fullscreen, and printing
to PDF produces one 16:9 page per slide.

## Build the PDF

From this directory:

```sh
make pdf
```

The PDF is written to `build/final-report.pdf`. Build the preliminary report with
`make pdf INPUT=preliminary-report.md`. The build directory also
contains generated LaTeX, rasterized figures, and compilation logs, all ignored
by Git. The Desktop PDF used during layout review is not a build dependency.

Requirements:

- Python 3 (standard library only)
- Pandoc 3 or newer and XeLaTeX with CTeX, `needspace`, and the standard Pandoc
  LaTeX packages
- Source Han Serif SC and JetBrains Mono fonts
- Chrome/Chromium, or `rsvg-convert`, for SVG figures. Set `REPORT_BROWSER` to
  the browser executable if it is not found automatically. An isolated headless
  profile under `build/` is used; your interactive browser profile is untouched.
- Noto Sans SC for the architecture SVG and a Chinese sans-serif font such as
  PingFang SC or Noto Sans SC for the ring SVG

The shared style in `rendering/` is adapted from the personal Pandoc CTeX report
template. It uses A4, 2 cm margins, 11 pt body text, 1.2 line spacing, a bold
compact title, and additional space around image/caption groups. Tables use
smaller type; headings and short tables reserve space before page breaks.

## Reuse for the final report

The pipeline accepts any report path and does not depend on preliminary-report
wording or figure numbers:

```sh
make pdf INPUT=final-report.md
# From the repository root, or for another report location:
python3 docs/report/rendering/build.py path/to/report.md --output path/to/report.pdf
```

Keep the title in YAML front matter. A trailing backslash creates a title line
break. Body headings start at `##`; Pandoc promotes them one level for the PDF.
Headings already contain their numbering, so automatic numbering is disabled.
Use `{.page-break}` on a heading when it should begin a new page.

Use standalone Markdown images with their complete caption as the alt text:

```md
![图 3-1　系统总体架构](assets/architecture.svg){#fig-architecture width=86%}
```

Give a table its numbered caption on a `Table:` line just above it; the PDF
prints the caption text as written, without an automatic label:

```md
Table: 表 3-2　三种写入确认策略 {#tbl-ack-policies}
```

The same caption serves the Markdown view and PDF.

Give every figure and table an identifier (`#fig-…`, `#tbl-…`) and refer to it
with a link whose text is the caption label, up to the first full-width space:

```md
各策略的差异见[表 3-2](#tbl-ack-policies)，整体结构如[图 3-1](#fig-architecture)所示。
```

In the PDF these are clickable internal links. The build stops with an error if
a link's text differs from the caption label (for example after renumbering) or
if it points to an identifier that does not exist, so a stale reference cannot
slip through. Section references work the same way through heading identifiers
(`### 4.2 …  {#sec-token-index}` and `[4.2 节](#sec-token-index)`); their text is
not checked. The rendering filter keeps
the image and caption together. Figure numbering and explanations belong in the
report, while typography belongs in the shared rendering files.

Pandoc citations are supported through `--citeproc`. A later report can set its
own `bibliography` and `csl` in YAML; relative paths resolve beside that report.

## Figure sources

- `assets/gen_ring_example.py` regenerates the transparent `ring-example.svg`:
  run `make figures` after editing it.
- `assets/architecture.svg` is the approved, manually laid-out vector drawing.
  `assets/architecture.mmd` records its Mermaid graph structure; it is not an
  exact layout generator for the SVG. Keep the two consistent when changing the
  architecture.

Generated PDFs are local build outputs. The report sources, vector figures, and
rendering files are the files to version.
