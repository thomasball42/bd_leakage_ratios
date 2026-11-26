# -*- coding: utf-8 -*-
"""
Where one country's leakage goes abroad, on a map, at one displacement size:
`python country_flow_map.py [ISO ...] [--coverage F] [--labels N] [--metric M] [--item ITEM] [--size S]
[--no-subtitle] [--config CONFIG]`
(default: GBR BRA, the flows that carry 95% of each one's leakage abroad, the 10
countries receiving the most labelled, extinctions, the production portfolio, size
1). One figure per displacing country C, in the scratch figures' profiles/ folder:

  flow_map_{ISO}_{item}_{metric}_size{S}.png

flow_map.py's map, of C's row only. Each flow is an arc from C's capital, ringed, to
that of the country where the leakage lands, growing wider and more opaque towards
it. C is every arc's source, so the colour is the region of the country where the
leakage lands, and the legend has each region's share. The arc's width, at the end,
and the area of the dot there are both the flow, as a share of all C leaks abroad
(the widths on a square-root scale, so that the small flows show). The largest that
carry --coverage of it are drawn, the smallest first, and every country that
receives any is a dot. The largest flow has the widest arc and the largest dot in
every figure, so it's the legend's shares, not the widths, that compare between
figures. The leakage that stays in C (what it replaces itself, and the feed grown
there for others) isn't shown: its share is in the subtitle.

The map and the capitals are _map.py's. With --no-subtitle (or --nosub), the grey
description under the title is left out.
"""

import argparse
from pathlib import Path

import numpy as np

import _common as c
import _map
from _map import DOT, SMALLEST_DOT, WIDEST

pf, fm, plt = c.pf, c.fm, c.plt
ALPHA = 0.2, 0.9  # the arcs' opacity, at their start and end: they overlap less than flow_map.py's, so from higher
RING = 64  # the area of the ring at C's capital, in points²
NICE = np.array([0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100])  # the legend's flows, in %

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("countries", nargs="*", default=["GBR", "BRA"], help="ISO3 codes (default: GBR BRA)")
parser.add_argument("--coverage", type=float, default=0.95,
                    help="the share of C's leakage abroad that the flows drawn carry, the largest first, or 1 for "
                         "every flow (default: 0.95)")
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
size = pf.size_text(m, m["sizes"][i])
isos = np.array(m["location_isos"])
region = c.region_of(list(isos))
colour = {**c.REGION_COLOURS, c.NO_REGION: c.MUTED}
names = c.attributes()["name"].dropna().to_dict()
where, placed = _map.capitals(list(isos))


def name(iso: str) -> str:
    """The World Bank's name for a country, or its code where it has none."""
    return names.get(iso, iso)


def percent(share: float) -> str:
    """A share as a %, to a decimal place, or <0.1% where it'd round to none."""
    return "<0.1%" if 0 < share < 0.0005 else f"{share:.1%}"


def amount(value: float) -> str:
    """A positive amount to 3 significant figures, as x × 10^n where it's below 0.01 or above 9,999."""
    power = int(np.floor(np.log10(value)))
    return f"{value:.3g}" if -2 <= power <= 3 else rf"{value / 10.0 ** power:.2f}$\times10^{{{power}}}$"


for iso in args.countries:
    if iso not in m["country_isos"]:
        print(f"No {iso} in the flow matrices of {c.item_label(m)}")
        continue
    leaked = np.nan_to_num(m[f"{args.metric}_leaked"][i, list(m["country_isos"]).index(iso)].astype(float))
    home = isos == iso
    flow = np.where(home, 0, leaked).clip(0)  # to each location abroad, per kg displaced
    total = flow.sum()
    negative = -leaked[~home & (leaked < 0)].sum()
    if negative:
        print(f"{iso}: negative cells abroad, set to 0: {negative / np.abs(leaked[~home]).sum():.2%} of their "
              "absolute sum")
    if not total > 0:
        print(f"{iso} leaks nothing abroad, or has no extensified result; skipping")
        continue
    source = _map.capitals([iso])
    if not source[1].all():
        print(f"{iso} has no capital; skipping")
        continue
    start = source[0][0]
    lost = (flow > 0) & ~placed
    if lost.any():
        print(f"{iso}: no capital: {', '.join(isos[lost])}, left out, with {flow[lost].sum() / total:.2%} of the "
              "leakage abroad")

    # the flows drawn: the largest, until they carry --coverage of the leakage, of those to countries placed
    into = np.argsort(-flow)
    into = into[(flow[into] > 0) & placed[into]]
    carried = np.cumsum(flow[into]) / total
    drawn = into[:min(len(into), np.searchsorted(carried, args.coverage - 1e-9) + 1)]
    print(f"{iso}: {len(drawn):,} flows of {len(into):,}, carrying {flow[drawn].sum() / total:.2%} of the leakage "
          "abroad")
    largest = flow[into[0]]

    def width(f: float) -> float:
        """An arc's width at its end, in points, for flow f."""
        return WIDEST * np.sqrt(f / largest)

    def area(f: np.ndarray) -> np.ndarray:
        """A dot's area, in points², for flow f."""
        return np.maximum(SMALLEST_DOT, DOT * f / largest)

    stays = leaked[home].sum() / leaked.sum()
    fig, ax, globe, legend = _map.frame(
        f"{name(iso)}, {c.item_label(m).lower()}: {pf.NAMES[args.metric]} leaking abroad, to {len(into):,} countries, "
        f"displacing {size}",
        "" if args.no_subtitle else
        f"The leakage abroad is {amount(total)} {pf.UNITS[args.metric]} per kg displaced. Each arc runs from the "
        f"displacing country's capital, ringed, to that of where the leakage lands, in that country's region's colour, "
        f"growing towards it; its width there, and the dot's area, are the flow. The {len(drawn):,} largest flows, "
        f"{flow[drawn].sum() / total:.0%} of the leakage abroad, are drawn. {stays:.0%} of all the leakage stays at "
        f"home, and isn't shown.")

    # the arcs, the smallest first, then the dots, the largest first, and C's ring
    _map.draw_arcs(ax, globe, [(start, where[k], width(flow[k]), colour[region[isos[k]]]) for k in drawn[::-1]], ALPHA)
    ax.scatter(*where[into].T, s=area(flow[into]), facecolor=[colour[region[k]] for k in isos[into]],
               edgecolor=pf.SURFACE, linewidth=0.6, zorder=2)
    ax.scatter(*start, s=RING, facecolor=pf.SURFACE, edgecolor=pf.INK, linewidth=1.8, zorder=3)

    # C, and the countries receiving the most, named, clear of each other and of the dots that show
    radii = np.sqrt(area(flow)) / 2
    ring = np.sqrt(RING) / 2 + 0.9
    _map.label(ax, [(start, ring, name(iso), "bold")]
          + [(where[k], radii[k], f"{name(isos[k])} {percent(flow[k] / total)}", "normal")
             for k in into[:args.labels]],
          [(start, ring)] + [(where[k], radii[k]) for k in into if radii[k] > 2])

    # the legend: the regions' colours and shares, and the arcs' widths and dots' areas
    legend.text(0, 1.9, "Where the leakage lands, by region, % of the leakage abroad", ha="left", va="top",
                fontsize=8.5, color=pf.INK_2)
    regions = {r: sum(flow[k] for k in range(len(isos)) if region[isos[k]] == r)
               for r in [*c.REGION_COLOURS, c.NO_REGION]}
    for p, (r, f) in enumerate((r, f) for r, f in regions.items() if f > 0):
        x, y = 2.65 * (p % 4), 1.1 - 0.7 * (p // 4)
        legend.plot([x, x + 0.35], [y, y], color=colour[r], linewidth=3, solid_capstyle="round")
        legend.text(x + 0.5, y, f"{c.GROUP_LABELS.get(r, r)} {percent(f / total)}", ha="left", va="center",
                    fontsize=8.5, color=pf.INK)
    legend.text(10.9, 1.9, "Flow, % of the leakage abroad: arc's end, dot's area", ha="left", va="top",
                fontsize=8.5, color=pf.INK_2)
    # the largest of NICE there is a flow of, and two smaller
    top = max(4, np.searchsorted(NICE, 100 * largest / total, side="right") - 1)
    for p, share in enumerate(NICE[[top - 4, top - 2, top]]):
        x = 10.95 + 1.4 * p
        f = share / 100 * total
        legend.plot([x, x + 0.45], [0.75, 0.75], color=pf.INK_2, linewidth=width(f), solid_capstyle="butt")
        legend.scatter([x + 0.45], [0.75], s=area(f), facecolor=pf.INK_2, edgecolor=pf.SURFACE, linewidth=0.6,
                       clip_on=False, zorder=3)
        legend.text(x + 0.82, 0.75, f"{share:g}%", ha="left", va="center", fontsize=8.5, color=pf.INK)

    c.save(fig, f"flow_map_{iso}_{c.stem(m)}_{args.metric}_size{m['sizes'][i]:.3g}.png", args.config,
           folder=c.PROFILES)
