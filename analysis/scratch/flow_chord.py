# -*- coding: utf-8 -*-
"""
Every country's leakage abroad at once, as a chord diagram, at one displacement
size: `python flow_chord.py [--coverage F] [--metric M] [--item ITEM] [--size S] [--config CONFIG]`
(default: the ribbons that carry 99% of the leakage across borders, extinctions, the
production portfolio, size 1).

Each country's row is weighted by its production, as if every country displaced
the same share of its production, each in its own scenario, as in group_flows.py.
Around the ring, every country, grouped by World Bank region (in the order of
_common.REGION_COLOURS, east from the Americas, and those with none last) and the
largest first within each: its arc is all the leakage it sends abroad and all it
receives from abroad. A ribbon runs from the displacing country, flush with its arc
and in its region's colour, to where the leakage lands, where it stops short of the
arc; its width at each end is the flow. The largest ribbons that carry --coverage
of the cross-border leakage are drawn, the others' slots on the arcs left empty.
The leakage that stays in the displacing country (what it replaces itself) isn't
shown: its share is in the subtitle.

The countries with arcs wide enough are labelled with their ISO3 codes, and the
legend gives each region's share of the cross-border leakage, out and in.
"""

import argparse
import textwrap
from pathlib import Path

import numpy as np
from matplotlib.collections import PolyCollection
from matplotlib.patches import Rectangle, Wedge

import _common as c

pf, fm, plt = c.pf, c.fm, c.plt
REGION_GAP = 0.005  # the gap between the regions, as a share of the ring
R = 1.0  # the ribbons' radius; the country arcs are outside it, from R + ARC_GAP to R + ARC_GAP + ARC_WIDTH
ARC_GAP, ARC_WIDTH = 0.012, 0.05
INSET = 0.025  # how far short of the arc a ribbon stops where its leakage lands
LABEL_SIZES = (5.5, 8)  # the ISO3 codes' font sizes, at most as tall as their arcs are wide: arcs narrower than
# the smallest aren't labelled
LIMIT = 1.32  # the ring's axes run from -LIMIT to LIMIT

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--coverage", type=float, default=0.99,
                    help="the share of the cross-border leakage that the ribbons drawn carry, the largest first, "
                         "or 1 for every ribbon (default: 0.99)")
parser.add_argument("--metric", choices=fm.METRICS, default="bd", help="(default: bd)")
parser.add_argument("--item", default=fm.PORTFOLIO, help="an item code or name, or portfolio (default)")
parser.add_argument("--size", type=float, default=1, help="the displacement size, the run's nearest (default: 1)")
parser.add_argument("--config", type=Path, default=c._run_paths.DEFAULT_CONFIG, help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()
if not 0 < args.coverage <= 1:
    parser.error("--coverage is a share, above 0 and at most 1")

m = c.load(args.item, args.config)
i = fm.size_index(m, args.size)
isos, flow, home = c.cross_border(m, args.metric, i)
n = len(isos)
region = c.region_of(isos)
regions = [r for r in [*c.REGION_COLOURS, c.NO_REGION] if r in region.values()]
colour = {**c.REGION_COLOURS, c.NO_REGION: c.MUTED}
out, into = flow.sum(axis=1), flow.sum(axis=0)
total = flow.sum()

# the ring, in shares of it from the top, clockwise: each region's countries, the largest first, with a gap
# between the regions
order = [k for r in regions for k in sorted((k for k in range(n) if region[isos[k]] == r), key=lambda k: -out[k] - into[k])]
scale = (1 - REGION_GAP * len(regions)) / (2 * total)
start, end = np.zeros(n), np.zeros(n)
spans = {}  # each region's start and end
t = REGION_GAP / 2
for r in regions:
    spans[r] = [t, None]
    for k in (k for k in order if region[isos[k]] == r):
        start[k], end[k] = t, t + (out[k] + into[k]) * scale
        t = end[k]
    spans[r][1] = t
    t += REGION_GAP
middle = (start + end) / 2

# each country's arc split into slots, one per flow out and one per flow in, so that no two ribbons cross: the
# partners ordered by how far clockwise they are from the arc's end, the furthest first, and each partner's flow
# out before its flow in. source[A, B] and target[A, B] are where the flow from A to B starts on A's arc, and
# lands on B's.
source, target = np.zeros((n, n)), np.zeros((n, n))
for k in range(n):
    t = start[k]
    for j in sorted((j for j in range(n) if j != k), key=lambda j: -((middle[j] - end[k]) % 1)):
        source[k, j], t = t, t + flow[k, j] * scale
        target[j, k], t = t, t + flow[j, k] * scale


def xy(t: np.ndarray, r: float) -> np.ndarray:
    """The points at shares t of the ring, clockwise from the top, at radius r."""
    angle = np.pi / 2 - 2 * np.pi * np.asarray(t)
    return np.column_stack([r * np.cos(angle), r * np.sin(angle)])


def arc(t0: float, t1: float, r: float) -> np.ndarray:
    return xy(np.linspace(t0, t1, max(2, int((t1 - t0) * 720))), r)


def curve(p0: np.ndarray, p1: np.ndarray, points: int = 30) -> np.ndarray:
    """The quadratic Bézier curve from p0 to p1 through the ring's centre."""
    s = np.linspace(0, 1, points)[:, None]
    return (1 - s) ** 2 * p0 + s ** 2 * p1


def ribbon(a: int, b: int) -> np.ndarray:
    """The ribbon from A's slot for B, flush with A's arc, to B's slot for A, inset."""
    s0, s1 = source[a, b], source[a, b] + flow[a, b] * scale
    t0, t1 = target[a, b], target[a, b] + flow[a, b] * scale
    return np.concatenate([arc(s0, s1, R), curve(xy(s1, R)[0], xy(t0, R - INSET)[0]), arc(t0, t1, R - INSET),
                           curve(xy(t1, R - INSET)[0], xy(s0, R)[0])])


# the ribbons drawn: the largest, until they carry --coverage of the leakage
cells = np.argsort(flow, axis=None)[::-1]
cells = cells[flow.flat[cells] > 0]
carried = np.cumsum(flow.flat[cells]) / total
drawn = cells[:min(len(cells), np.searchsorted(carried, args.coverage - 1e-9) + 1)]
print(f"{len(drawn):,} ribbons of {len(cells):,}, carrying {flow.flat[drawn].sum() / total:.2%} of the leakage "
      "across borders")

# the title and subtitle, wrapped to the figure's width
width = 12.0
size = pf.size_text(m, m["sizes"][i])
heading = textwrap.wrap(f"{c.item_label(m)}: {pf.NAMES[args.metric]} leaking across borders, {len(cells):,} flows "
                        f"between {n} countries, displacing {size}", int((width - 0.6) * 72 / (14 * 0.58)))
subheading = textwrap.wrap(
    f"Every country displacing the same share of its production, each in its own scenario. Each ribbon runs from the displacing country, in its region's colour, to where the "
    f"leakage lands, where it stops short of the arc; each country's arc is all it leaks abroad and receives from "
    f"abroad. The {len(drawn):,} largest ribbons, {flow.flat[drawn].sum() / total:.0%} of the leakage across "
    f"borders, are drawn. {home:.0%} of all the leakage stays in the displacing country, and isn't shown.",
    int((width - 0.6) * 72 / (9 * 0.58)))
header = 0.25 + 0.3 * len(heading) + 0.08 + 0.19 * len(subheading) + 0.1  # inches
legend_height = 1.0
ring = width - 0.4
height = header + ring + legend_height
fig = plt.figure(figsize=(width, height), facecolor=pf.SURFACE)
fig.text(0.3 / width, 1 - 0.25 / height, "\n".join(heading), ha="left", va="top", color=pf.INK, fontsize=14,
         linespacing=1.25)
fig.text(0.3 / width, 1 - (0.25 + 0.3 * len(heading) + 0.08) / height, "\n".join(subheading), ha="left", va="top",
         color=pf.INK_2, fontsize=9, linespacing=1.4)
ax = fig.add_axes((0.2 / width, legend_height / height, ring / width, ring / height))
ax.set_aspect("equal")
ax.set_xlim(-LIMIT, LIMIT)
ax.set_ylim(-LIMIT, LIMIT)
points = ring * 72 / (2 * LIMIT)  # points per unit of radius
ax.axis("off")

# the ribbons, the smallest first, so that the largest are on top
ribbons = PolyCollection([ribbon(*divmod(cell, n)) for cell in drawn[::-1]],
                         facecolors=[colour[region[isos[cell // n]]] for cell in drawn[::-1]],
                         edgecolors="none", alpha=0.62, zorder=1)
ax.add_collection(ribbons)

# the country arcs, with a hairline of surface between those wide enough to show one, and their ISO3 codes
inner = R + ARC_GAP
for k in range(n):
    if end[k] <= start[k]:
        continue
    theta0, theta1 = 90 - 360 * end[k], 90 - 360 * start[k]
    wide = end[k] - start[k] > 0.0012
    ax.add_patch(Wedge((0, 0), inner + ARC_WIDTH, theta0, theta1, width=ARC_WIDTH, facecolor=colour[region[isos[k]]],
                       edgecolor=pf.SURFACE if wide else "none", linewidth=0.6, zorder=2))
    label = min(LABEL_SIZES[1], 0.75 * 2 * np.pi * (inner + ARC_WIDTH) * (end[k] - start[k]) * points)
    if label >= LABEL_SIZES[0]:
        angle = 90 - 360 * middle[k]
        flip = np.cos(np.radians(angle)) < 0  # on the left, read outwards from the right
        x, y = xy(middle[k], inner + ARC_WIDTH + 0.012)[0]
        ax.text(x, y, isos[k], rotation=angle + 180 if flip else angle, rotation_mode="anchor",
                ha="right" if flip else "left", va="center", fontsize=label,
                color=pf.INK, zorder=3)

# the regions' names, outside the codes, along a hairline that spans each region
outer = inner + ARC_WIDTH + 0.115
for r, (t0, t1) in spans.items():
    line = arc(t0, t1, outer)
    ax.plot(line[:, 0], line[:, 1], color=colour[r], linewidth=2.5, solid_capstyle="butt", zorder=2)
    t = (t0 + t1) / 2
    angle = -360 * t  # tangent to the ring, upright
    if np.sin(np.radians(90 - 360 * t)) < 0:
        angle += 180
    x, y = xy(t, outer + 0.04)[0]
    ax.text(x, y, c.GROUP_LABELS.get(r, r), rotation=angle, rotation_mode="anchor", ha="center", va="center",
            fontsize=10, color=pf.INK, zorder=3)

# the legend: each region's colour, and its share of the cross-border leakage, out and in
legend = fig.add_axes((0.3 / width, 0.1 / height, (width - 0.6) / width, (legend_height - 0.2) / height))
legend.axis("off")
legend.set_xlim(0, 4)
legend.set_ylim(0, 2)
for p, r in enumerate(regions):
    x, y = p % 4, 1.5 - p // 4
    members = [k for k in range(n) if region[isos[k]] == r]
    legend.add_patch(Rectangle((x, y - 0.15), 0.06, 0.3, facecolor=colour[r], edgecolor="none"))
    legend.text(x + 0.1, y + 0.08, c.GROUP_LABELS.get(r, r), ha="left", va="center", fontsize=9, color=pf.INK)
    legend.text(x + 0.1, y - 0.22, f"{len(members)} countries · sends {out[members].sum() / total:.1%} · "
                f"receives {into[members].sum() / total:.1%}", ha="left", va="center", fontsize=7.5, color=pf.INK_2)

c.save(fig, f"flow_chord_{c.stem(m)}_{args.metric}_size{m['sizes'][i]:.3g}.png", args.config, folder=c.FLOWS)
