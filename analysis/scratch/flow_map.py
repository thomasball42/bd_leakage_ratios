# -*- coding: utf-8 -*-
"""
Every country's leakage abroad at once, on a map, at one displacement size:
`python flow_map.py [--coverage F] [--labels N] [--metric M] [--item ITEM] [--size S] [--no-subtitle]
[--config CONFIG]`
(default: the flows that carry 95% of the leakage across borders, the 10 countries
receiving the most labelled, extinctions, the production portfolio, size
1).

Each country's row is weighted by its production, as if every country displaced
the same share of its production, each in its own scenario, as in group_flows.py.
Each flow is an arc from the displacing country's capital to that of the country
where the leakage lands, bowing to the left of its direction (by at most _map.BOW), so
that the flows each way between two countries part, and growing wider and more
opaque towards where it lands. Its colour is the displacing country's region's (as
in flow_chord.py), and its width, at the end, the flow, on a square-root scale, so
that the small flows show. The largest that carry --coverage of the leakage across
borders are drawn, the smallest first, within the globe's edge. Each country is a dot at its capital, its
area what it receives from abroad. The leakage that stays in the displacing country
(what it replaces itself) isn't shown: its share is in the subtitle.

The map and the capitals are _map.py's. country_flow_map.py is the same for one
displacing country.

With --no-subtitle (or --nosub), the grey description under the title is left
out.
"""

import argparse
from pathlib import Path

import numpy as np
from matplotlib import patheffects

import _common as c
import _map
from _map import DOT, DOT_ALPHA, DOTS, SMALLEST_DOT, WIDEST

pf, fm, plt = c.pf, c.fm, c.plt

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--coverage", type=float, default=0.95,
                    help="the share of the cross-border leakage that the flows drawn carry, the largest first, or 1 "
                         "for every flow (default: 0.95)")
parser.add_argument("--labels", type=int, default=10, help="the countries receiving the most labelled (default: 10)")
parser.add_argument("--metric", choices=fm.METRICS, default="bd", help="(default: bd)")
parser.add_argument("--item", default=fm.PORTFOLIO, help="an item code or name, or portfolio (default)")
parser.add_argument("--size", type=float, default=1, help="the displacement size, the run's nearest (default: 1)")
parser.add_argument("--no-subtitle", "--nosub", action="store_true",
                    help="leave out the grey description under the title")
parser.add_argument("--config", type=Path, default=c._run_paths.DEFAULT_CONFIG, help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()
if not 0 < args.coverage <= 1:
    parser.error("--coverage is a share, above 0 and at most 1")

m = c.load(args.item, args.config)
i = fm.size_index(m, args.size)
isos, flow, home = c.cross_border(m, args.metric, i)
n = len(isos)
region = c.region_of(isos)
colour = {**c.REGION_COLOURS, c.NO_REGION: c.MUTED}
total = flow.sum()
into = flow.sum(axis=0)

where, placed = _map.capitals(isos)
if not placed.all():
    lost = flow[~placed].sum() + flow[:, ~placed].sum() - flow[np.ix_(~placed, ~placed)].sum()
    print(f"No capital: {', '.join(np.array(isos)[~placed])}, left out, with {lost / total:.2%} of the leakage")

# the flows drawn: the largest, until they carry --coverage of the leakage, of those between countries placed
cells = np.argsort(flow, axis=None)[::-1]
cells = cells[(flow.flat[cells] > 0) & placed[cells // n] & placed[cells % n]]
carried = np.cumsum(flow.flat[cells]) / total
drawn = cells[:min(len(cells), np.searchsorted(carried, args.coverage - 1e-9) + 1)]
print(f"{len(drawn):,} flows of {len(cells):,}, carrying {flow.flat[drawn].sum() / total:.2%} of the leakage "
      "across borders")
largest = flow.flat[drawn[0]]


def width(f: np.ndarray) -> np.ndarray:
    """An arc's width at its end, in points, for flow f."""
    return WIDEST * np.sqrt(f / largest)


size = pf.size_text(m, m["sizes"][i])
fig, ax, globe, legend = _map.frame(
    f"{c.item_label(m)}: {pf.NAMES[args.metric]} leaking abroad, {len(cells):,} flows between {n} countries, "
    f"displacing {size}",
    "" if args.no_subtitle else
    f"Every country displacing the same share of its production, each in its own scenario. Each arc runs from the "
    f"displacing country's capital, in its region's colour, to that of where the leakage lands, growing towards it; "
    f"its width there is the flow. The {len(drawn):,} largest flows, {flow.flat[drawn].sum() / total:.0%} of the "
    f"leakage across borders, are drawn. Each dot's area is what the country receives from abroad. {home:.0%} of "
    f"all the leakage stays in the displacing country, and isn't shown.")

# the arcs, the smallest first, then the dots
_map.draw_arcs(ax, globe, [(where[a], where[b], width(flow[a, b]), colour[region[isos[a]]])
                           for a, b in (divmod(cell, n) for cell in drawn[::-1])])
ax.scatter(*where[placed].T, s=np.maximum(SMALLEST_DOT, DOT * into[placed] / into.max()), facecolor=DOTS,
           alpha=DOT_ALPHA, edgecolor=pf.SURFACE, linewidth=0.6, zorder=2)

# the countries receiving the most, named
names = c.attributes()["name"].dropna().to_dict()
halo = [patheffects.withStroke(linewidth=2.5, foreground=pf.SURFACE)]
for k in [k for k in np.argsort(-into) if placed[k]][:args.labels]:
    radius = np.sqrt(max(SMALLEST_DOT, DOT * into[k] / into.max()) / np.pi)  # the dot's, in points
    ax.annotate(f"{names.get(isos[k], isos[k])} {into[k] / total:.1%}", where[k], xytext=(radius * 0.75 + 3, 3),
                textcoords="offset points", ha="left", va="bottom", fontsize=8.5, color=pf.INK, path_effects=halo,
                zorder=3)

# the legend: the displacing regions' colours, the arcs' widths and the dots' areas
legend.text(0, 1.9, "From the displacing country's region", ha="left", va="top", fontsize=8.5, color=pf.INK_2)
regions = [r for r in [*c.REGION_COLOURS, c.NO_REGION] if r in region.values()]
for p, r in enumerate(regions):
    x, y = 2.05 * (p % 4), 1.1 - 0.7 * (p // 4)
    legend.plot([x, x + 0.35], [y, y], color=colour[r], linewidth=3, solid_capstyle="round")
    legend.text(x + 0.5, y, c.GROUP_LABELS.get(r, r), ha="left", va="center", fontsize=8.5, color=pf.INK)
legend.text(8.6, 1.9, "Flow, at its end, % of the leakage across borders", ha="left", va="top", fontsize=8.5,
            color=pf.INK_2)
for p, share in enumerate([0.1, 1, 5]):
    x = 8.6 + 1.1 * p
    legend.plot([x, x + 0.4], [0.75, 0.75], color=pf.INK_2, linewidth=width(np.array(share / 100 * total)),
                solid_capstyle="butt")
    legend.text(x + 0.5, 0.75, f"{share:g}%", ha="left", va="center", fontsize=8.5, color=pf.INK)
legend.text(12.3, 1.9, "Received from abroad, %", ha="left", va="top", fontsize=8.5, color=pf.INK_2)
for p, share in enumerate([1, 5, 15]):
    x = 12.45 + 0.95 * p
    legend.scatter([x], [0.75], s=DOT * share / 100 * total / into.max(), facecolor=DOTS, alpha=DOT_ALPHA,
                   edgecolor=pf.SURFACE, linewidth=0.6, clip_on=False)
    legend.text(x + 0.3, 0.75, f"{share:g}%", ha="left", va="center", fontsize=8.5, color=pf.INK)

c.save(fig, f"flow_map_{c.stem(m)}_{args.metric}_size{m['sizes'][i]:.3g}.png", args.config, folder=c.FLOWS)
