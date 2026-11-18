"""Generate the evidence figures of the testing chapter.

Every value below is taken from a recorded measurement in ``docs/``:
``fault-under-load.md`` (owner failover), ``scaling-under-load.md`` (the 9 to 10
growth steps) and ``acceptance-metrics.md`` (the balance of the virtual node
count).  The script only draws those numbers; it does not invent any.
"""

import math
from pathlib import Path

INK = '#1b2430'
MUTED = '#5d6874'
RULE = '#e3dfd7'
PAPER = '#fff'
BLUE = '#1f6f8b'
ORANGE = '#c0532b'
GREEN = '#3d7d4f'
GOLD = '#a07a12'
FONT = 'font-family="PingFang SC, Noto Sans SC, Microsoft YaHei, system-ui, sans-serif"'

HERE = Path(__file__).resolve().parent


def text(x, y, s, size=13, fill=INK, weight=500, anchor='middle', extra=''):
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" '
            f'fill="{fill}" text-anchor="{anchor}" {FONT} {extra}>{s}</text>')


def line(x1, y1, x2, y2, stroke=INK, width=1.4, extra=''):
    return (f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'stroke="{stroke}" stroke-width="{width}" {extra}/>')


def rect(x, y, w, h, fill, rx=3, extra=''):
    return (f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" rx="{rx}" '
            f'fill="{fill}" {extra}/>')


def svg(width, height, body, label):
    head = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
            f'width="{width}" height="{height}" role="img" aria-label="{label}">')
    return '\n'.join([head] + body + ['</svg>'])


def write(name, content):
    (HERE / name).write_text(content, encoding='utf-8')


# --------------------------------------------------------------------------
# Figure 6-1: the recovery timeline of an owner loss.
# --------------------------------------------------------------------------
def failover_timeline():
    width, height = 1000, 300
    x0, x1 = 215, 960
    tmax = 24.0

    def x_of(t):
        return x0 + (t / tmax) * (x1 - x0)

    rows = [
        ('OwnerOnly（3 次运行的中位数）',
         [('发布新拓扑 5.63 s', 5.63, BLUE),
          ('首个成功读/写 5.80 s', 5.80, GREEN),
          ('恢复到满 RF 15.97 s', 15.97, ORANGE)], 96),
        ('FirstSuccessor（1 次运行）',
         [('发布新拓扑 5.05 s', 5.05, BLUE),
          ('首个成功读 5.15 s', 5.15, GREEN),
          ('首个成功写 13.85 s', 13.85, GOLD),
          ('恢复到满 RF 22.78 s', 22.78, ORANGE)], 200),
    ]

    body = []
    body.append(rect(x0, 44, x_of(5) - x0, 214, RULE, 0, 'opacity=".55"'))
    body.append(text((x0 + x_of(5)) / 2, 36, '5 s 租约到期', 12, MUTED, 600))
    body.append(line(x_of(5), 44, x_of(5), 258, MUTED, 1.2, 'stroke-dasharray="4 4"'))

    for title, marks, y in rows:
        body.append(text(x0 - 14, y + 5, title, 13, INK, 600, 'end'))
        body.append(line(x0, y, x1, y, RULE, 3, 'stroke-linecap="round"'))
        for index, (label, t, color) in enumerate(marks):
            x = x_of(t)
            body.append(line(x, y - 9, x, y + 9, color, 2))
            body.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5.5" fill="{color}"/>')
            above = index % 2 == 0
            body.append(text(x, y - 22 if above else y + 30, label, 12.5,
                             INK if above else MUTED, 600))

    body.append(line(x0, 262, x1, 262, INK, 1.2))
    for t in range(0, 25, 5):
        x = x_of(t)
        body.append(line(x, 262, x, 269, INK, 1.2))
        body.append(text(x, 286, f'{t} s', 12, MUTED, 500))
    body.append(text(x1, 286, '自 kill 起的时间', 12, MUTED, 500, 'end'))
    return svg(width, height, body, 'owner 故障后的恢复时间线')


# --------------------------------------------------------------------------
# Figure 6-2: the work steps of one 9 to 10 growth.
# --------------------------------------------------------------------------
def growth_steps():
    width, height = 1000, 330
    label_x = 225
    bar_x = 240
    bar_max = 690
    vmax = 2200.0
    items = [
        ('快照复制', 1021, BLUE),
        ('变更日志回放', 2144, BLUE),
        ('停写', 11, BLUE),
        ('校验', 1026, BLUE),
        ('发布', 16, BLUE),
        ('激活', 667, BLUE),
        ('客户端观测的停写窗口', 1098, ORANGE),
    ]

    body = []
    top = 44
    row = 38
    for tick in range(0, 2001, 500):
        x = bar_x + bar_max * tick / vmax
        body.append(line(x, top - 12, x, top + row * len(items) - 6, RULE, 1, 'stroke-dasharray="3 4"'))
        body.append(text(x, top + row * len(items) + 14, f'{tick}', 11.5, MUTED, 500))
    body.append(text(bar_x + bar_max, top + row * len(items) + 36, '毫秒', 12, MUTED, 500, 'end'))

    for index, (name, value, color) in enumerate(items):
        y = top + index * row
        w = bar_max * value / vmax
        body.append(text(label_x, y + 16, name, 13, INK, 500, 'end'))
        body.append(rect(bar_x, y, max(w, 2.0), 21, color, 3, 'opacity=".9"'))
        body.append(text(bar_x + max(w, 2.0) + 9, y + 16, f'{value} ms', 12, MUTED, 600, 'start'))
    return svg(width, height, body, '9 到 10 扩容各步骤的耗时')


# --------------------------------------------------------------------------
# Figure 6-3: the balance of the virtual node count.
# --------------------------------------------------------------------------
def balance_curve():
    width, height = 1000, 330
    x0, x1 = 95, 940
    y0, y1 = 44, 252
    vmin, vmax = 1.0, 400.0
    cmin, cmax = 0.03, 2.2
    measured = [(1, 1.412), (16, 0.178), (64, 0.139), (128, 0.088), (200, 0.062), (256, 0.048)]

    def x_of(v):
        return x0 + (math.log10(v) - math.log10(vmin)) / (math.log10(vmax) - math.log10(vmin)) * (x1 - x0)

    def y_of(c):
        return y1 - (math.log10(c) - math.log10(cmin)) / (math.log10(cmax) - math.log10(cmin)) * (y1 - y0)

    body = []
    for tick in (0.05, 0.1, 0.2, 0.5, 1.0, 2.0):
        y = y_of(tick)
        body.append(line(x0, y, x1, y, RULE, 1, 'stroke-dasharray="3 4"'))
        body.append(text(x0 - 12, y + 4, f'{tick:g}', 12, MUTED, 500, 'end'))
    for tick in (1, 4, 16, 64, 256):
        x = x_of(tick)
        body.append(line(x, y0, x, y1, RULE, 1, 'stroke-dasharray="3 4"'))
        body.append(text(x, y1 + 24, f'{tick}', 12, MUTED, 500))
    body.append(text((x0 + x1) / 2, y1 + 52, '每个物理节点的虚拟节点数 V', 12.5, INK, 600))
    body.append(text(x0 - 62, (y0 + y1) / 2, '拥有键数的变异系数 CV', 12.5, INK, 600,
                     'start', f'transform="rotate(-90 {x0 - 62:.1f} {(y0 + y1) / 2:.1f})"'))

    target = y_of(0.05)
    body.append(line(x0, target, x1, target, ORANGE, 1.6, 'stroke-dasharray="6 4"'))
    body.append(text(x0 + 12, target - 10, 'CV = 0.05（阶段 3 的目标按 CV 口径约需 V ≈ 400）',
                     12, ORANGE, 600, 'start'))

    theory = [(v, 1.0 / math.sqrt(v)) for v in (1, 4, 16, 64, 128, 256)]
    points = ' '.join(f'{x_of(v):.1f},{y_of(c):.1f}' for v, c in theory)
    body.append(f'<polyline points="{points}" fill="none" stroke="{MUTED}" stroke-width="1.6" '
                'stroke-dasharray="6 4"/>')
    body.append(text(x_of(9), y_of(1.0 / 3) + 4, '理论 1/√V', 12, MUTED, 600, 'start'))

    points = ' '.join(f'{x_of(v):.1f},{y_of(c):.1f}' for v, c in measured)
    body.append(f'<polyline points="{points}" fill="none" stroke="{BLUE}" stroke-width="2"/>')
    for v, c in measured:
        body.append(f'<circle cx="{x_of(v):.1f}" cy="{y_of(c):.1f}" r="5" fill="{BLUE}"/>')
    body.append(text(x_of(1) + 12, y_of(1.412) - 8, 'V = 1，CV = 1.412', 12, INK, 600, 'start'))
    body.append(text(x_of(128) - 14, y_of(0.088) + 18, 'V = 128，CV = 0.088', 12, INK, 600, 'end'))
    return svg(width, height, body, '虚拟节点数与节点份额的均衡度')


write('fig-owner-failover.svg', failover_timeline())
write('fig-growth-steps.svg', growth_steps())
write('fig-balance-vnodes.svg', balance_curve())
