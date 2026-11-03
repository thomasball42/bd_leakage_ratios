# -*- coding: utf-8 -*-
"""
Leakage between countries, at one displacement size: `python country_flows.py [--top N] [--include ISO ...]
[--metric M] [--item ITEM] [--size S] [--config CONFIG]` (default: the 30 countries with the most impact
crossing their borders, extinctions, the production portfolio, the smallest size).

group_flows.py's three panels, one figure each, with countries in place of
groups, each country's row weighted by its production, as if every country
displaced the same share of its production, each in its own scenario. With
--top N: the N countries with the most impact leaked or spared across their
borders, out and in, and any --include, with the rest of the world summed into
one row and column, and the larger values written in their cells. With --top 0:
every country. The countries are ordered by World Bank region, then by GDP per
capita, highest first.

The values span several orders of magnitude, so the colour scales are
logarithmic: where the leakage lands from 0.1% to 100% (blank below, or where
negative), and the net change and net direction symmetric about 0, to the 99th
percentile of their absolute values (beyond which they're at the scale's ends, and
written in), linear within 1/1000 of that. Rows that leak, or spare, nothing or less are blank
in where the leakage lands, or the net change. A row whose leakage sums to little
from large parts of either sign (Korea's: +110% to the USA, -170% to Australia,
with its negative pasture beta) has shares beyond 100%, at the scale's end.
"""

import argparse
from pathlib import Path

import numpy as np
from matplotlib.cm import ScalarMappable
from matplotlib.colors import LogNorm, SymLogNorm

import _common as c

pf, fm, plt = c.pf, c.fm, c.plt
REST = "Rest of world"
NO_REGION = "No region"
FLOOR = 1e-3  # the smallest share of where the leakage lands that's coloured

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--top", type=int, default=30, help="the countries shown, or 0 for every one (default: 30)")
parser.add_argument("--include", nargs="*", default=[], help="ISO3 codes to show as well as the top N")
parser.add_argument("--metric", choices=fm.METRICS, default="bd", help="(default: bd)")
parser.add_argument("--item", default=fm.PORTFOLIO, help="an item code or name, or portfolio (default)")
parser.add_argument("--size", type=float, default=None, help="the displacement size (default: the smallest)")
parser.add_argument("--config", type=Path, default=c._run_paths.DEFAULT_CONFIG, help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()

m = c.load(args.item, args.config)
i = fm.size_index(m, args.size)
attributes = c.attributes()
regions = c.GROUPINGS["region"][1]
region_of = {iso: region for iso, region in attributes["wb_region"].dropna().items() if region in regions}
gdp = attributes["gdp_pc"]
isos = sorted(set(m["country_isos"]) | set(m["location_isos"]))
unknown = sorted(set(args.include) - set(isos))
if unknown:
    parser.error(f"Not in the flow matrices of {c.item_label(m)}: {', '.join(unknown)}")

leaked, spared = c.flows(m, args.metric, i, {iso: iso for iso in isos}, isos)
crossing = np.abs(leaked) + np.abs(spared)
np.fill_diagonal(crossing, 0)
crossing = crossing.sum(axis=0) + crossing.sum(axis=1)  # each country's impact across its borders, out and in
if args.top:
    chosen = [isos[k] for k in np.argsort(-crossing)[:args.top]]
    chosen += [iso for iso in args.include if iso not in chosen]
    covered = crossing[[isos.index(iso) for iso in chosen]].sum() / crossing.sum()
else:
    chosen = isos


def order(iso: str) -> tuple:
    """By region (those with none last), then GDP per capita, highest first (those with none last)."""
    region = region_of.get(iso)
    return regions.index(region) if region else len(regions), -np.nan_to_num(gdp.get(iso, np.nan)), iso


def percent(v: float, signed: bool) -> str:
    """v as a %, to 2 significant figures, or none after the point from 100%."""
    x = v * 100
    return f"{x:{'+' if signed else ''}.{'0f' if abs(x) >= 99.5 else '2g'}}%"


chosen = sorted(chosen, key=order)
units = chosen + ([REST] if args.top else [])
if args.top:
    leaked, spared = c.flows(m, args.metric, i, {iso: iso if iso in chosen else REST for iso in isos}, units)
else:
    leaked, spared = (values[np.ix_(*[[isos.index(iso) for iso in units]] * 2)] for values in (leaked, spared))
panels = c.flow_panels(leaked, spared)
blocks = [c.GROUP_LABELS.get(region_of.get(iso), region_of.get(iso)) or NO_REGION for iso in chosen]
blocks += [REST] if args.top else []

n = len(units)
labelled = bool(args.top)  # values in the cells, and readable tick labels
label_size = 7 if n <= 40 else 3.2
size = pf.size_text(m, m["sizes"][i])
for key, (title, unit) in c.FLOW_PANELS.items():
    values = panels[key]
    finite = values[np.isfinite(values)]
    if key == "lands":
        norm, cmap, threshold = LogNorm(FLOOR, 1), c.BLUES.copy(), 0.01
        ticks = [1e-3, 1e-2, 1e-1, 1]
    else:
        limit = np.percentile(np.abs(finite), 99)  # a few rows that spare little reach far beyond the rest
        norm, cmap = SymLogNorm(limit / 1000, vmin=-limit, vmax=limit, base=10), c.DIVERGING.copy()
        threshold = 0.05 if key == "net" else np.abs(finite).max() / 20
        decades = 10.0 ** np.arange(np.ceil(np.log10(limit / 1000)), np.floor(np.log10(limit)) + 1)
        ticks = sorted([0, *decades, *-decades])
    cmap.set_bad(pf.SURFACE)
    if key == "lands":
        cmap.set_under(pf.SURFACE)  # below FLOOR; the diverging scales keep their ends beyond their limits

    side = 10 if labelled else 13
    fig = plt.figure(figsize=(side + 2.8, side + 2.2), facecolor=pf.SURFACE)
    ax = fig.add_axes([0.07, 0.09, 0.72, 0.7] if labelled else [0.07, 0.05, 0.72, 0.76])
    ax.set_facecolor(pf.SURFACE)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.imshow(values, cmap=cmap, norm=norm, interpolation="nearest")
    if labelled:  # the surface gap between cells
        ax.set_xticks(np.arange(n + 1) - 0.5, minor=True)
        ax.set_yticks(np.arange(n + 1) - 0.5, minor=True)
        ax.grid(which="minor", color=pf.SURFACE, linewidth=1.5)
    ax.tick_params(which="both", length=0, colors=pf.INK, labelsize=label_size)
    ax.set_xticks(range(n), units, rotation=90)
    ax.set_yticks(range(n), units)
    ax.set_xlabel("Where the impact lands (B)", color=pf.INK_2, fontsize=9)
    ax.set_ylabel("Displacing country (A)", color=pf.INK_2, fontsize=9)

    # the regions: lines between them, and their names to the right of their rows and, angled so that narrow
    # blocks' names don't collide, above their columns
    starts = [0] + [k for k in range(1, n) if blocks[k] != blocks[k - 1]]
    for start, end in zip(starts, starts[1:] + [n]):
        if start:
            ax.axhline(start - 0.5, color=pf.INK_2, linewidth=0.6)
            ax.axvline(start - 0.5, color=pf.INK_2, linewidth=0.6)
        if blocks[start] != REST:
            middle = (start + end - 1) / 2
            ax.text(n - 0.5 + n * 0.012, middle, blocks[start], ha="left", va="center", fontsize=7.5,
                    color=pf.INK_2, clip_on=False)
            ax.text(middle, -0.5 - n * 0.012, blocks[start], ha="left", va="bottom", rotation=45,
                    rotation_mode="anchor", fontsize=7.5, color=pf.INK_2, clip_on=False)

    if labelled:
        for a in range(n):
            for b in range(n):
                v = values[a, b]
                if np.isfinite(v) and abs(v) >= threshold:
                    shade = norm(v)
                    dark = shade > 0.65 if key == "lands" else abs(shade - 0.5) > 0.33
                    ax.text(b, a, percent(v, key != "lands"), ha="center", va="center", fontsize=5,
                            color="#ffffff" if dark else pf.INK)

    cax = fig.add_axes([0.93, 0.25, 0.012, 0.45])
    bar = fig.colorbar(ScalarMappable(norm, cmap), cax=cax, ticks=ticks)
    bar.outline.set_visible(False)
    bar.ax.tick_params(which="both", length=0, labelsize=7, colors=pf.INK_2)
    bar.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v * 100:.3g}%" if v else "0"))
    bar.ax.yaxis.set_minor_formatter(plt.FuncFormatter(lambda *_: ""))
    bar.set_label(unit.format(unit="country"), color=pf.INK_2, fontsize=8)

    if labelled:
        selection = (f"The {args.top} countries with the most {pf.NAMES[args.metric]} crossing their borders, out and "
                     f"in{' (and ' + ', '.join(args.include) + ')' if args.include else ''}: {covered:.0%} of all "
                     f"that crosses borders. The rest of the world is summed. Values of at least "
                     f"{threshold * 100:.2g}% are written in.")
    else:
        selection = "Every country."
    fig.text(0.07, 0.975, f"{title}: {c.item_label(m).lower()}, {pf.NAMES[args.metric]}, between countries, "
             f"displacing {size}", color=pf.INK, fontsize=12, va="top")
    fig.text(0.07, 0.945, f"{selection}\nEvery country displacing the same share of its production, in its own "
             "scenario. Ordered by World Bank region, then GDP per capita. Leakage includes what each country "
             "replaces itself.", color=pf.INK_2, fontsize=8.5, va="top", linespacing=1.5)
    c.save(fig, f"country_flows_{key}_{f'top{args.top}' if args.top else 'all'}_{c.stem(m)}_{args.metric}"
                f"_size{m['sizes'][i]:.3g}.png", args.config)
