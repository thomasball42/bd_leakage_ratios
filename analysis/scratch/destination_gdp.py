# -*- coding: utf-8 -*-
"""
Does leakage move down the income gradient? `python destination_gdp.py [--metric M] [--item ITEM] [--size S]
[--panels P ...] [--annotate A ...] [--config CONFIG]` (default: extinctions, the production portfolio, the smallest
size, all three panels). One point per displacing country C, sized by its production, in these panels, or only
those given with --panels (drawn in this order, whatever order they're given in):

  production  Where the production goes: C's GDP per capita (x) against the
              mean GDP per capita of the places abroad that replace its
              production, weighted by the share each replaces
  impact      Where the impact goes: the same, weighted by the impact each
              takes (the metric leaked there; negative leakage, as where a
              pasture beta is negative, counts as none)
  compare     Impact against production: the two means against each other:
              below y = x, the impact lands in poorer places than the
              production does

The means are geometric (of log GDP), and leave out C itself, so they're of the
leakage abroad. Countries with less than 90% of the weight on places with a GDP
per capita (or with none of their own) are left out. Each panel has the line of
best fit, least squares of log y on log x, with its slope.

--annotate labels the 8 largest producers and the 6 furthest from y = x in each
panel (furthest, the default), every country (all), none, or only the countries
given by ISO3 code, e.g. --annotate GBR BRA.
"""

import argparse
import textwrap
from pathlib import Path

import numpy as np
from matplotlib.ticker import FuncFormatter

import _common as c

pf, fm, plt = c.pf, c.fm, c.plt
MIN_KNOWN = 0.9  # the least share of the weight on places with a GDP per capita
PANELS = ["production", "impact", "compare"]
ANNOTATE = ["furthest", "all", "none"]

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--metric", choices=fm.METRICS, default="bd", help="(default: bd)")
parser.add_argument("--item", default=fm.PORTFOLIO, help="an item code or name, or portfolio (default)")
parser.add_argument("--size", type=float, default=None, help="the displacement size (default: the smallest)")
parser.add_argument("--panels", nargs="+", choices=PANELS, default=PANELS, help="the panels drawn (default: all three)")
parser.add_argument("--annotate", nargs="+", default=["furthest"], metavar="A",
                    help="the countries labelled: furthest (the 8 largest producers and the 6 furthest from y = x), "
                         "all, none, or ISO3 codes, e.g. GBR BRA (default: furthest)")
parser.add_argument("--config", type=Path, default=c._run_paths.DEFAULT_CONFIG, help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()
annotate = [a if a in ANNOTATE else a.upper() for a in args.annotate]
if len(annotate) > 1 and set(annotate) & set(ANNOTATE):
    parser.error("--annotate takes one of furthest, all and none, or ISO3 codes")

m = c.load(args.item, args.config)
i = fm.size_index(m, args.size)
gdp = c.attributes()["gdp_pc"]
own = gdp.reindex(m["country_isos"]).to_numpy()
log_gdp = np.log(gdp.reindex(m["location_isos"]).to_numpy())
known = np.isfinite(log_gdp)
abroad = ~fm.self_cells(m)


def destination_gdp(weights: np.ndarray) -> np.ndarray:
    """Each row's weighted geometric mean of the locations' GDP per capita abroad,
    NaN where less than MIN_KNOWN of its weight is on locations with one."""
    w = np.clip(np.nan_to_num(weights), 0, None) * abroad
    total, on_known = w.sum(axis=1), (w * known).sum(axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        mean = np.exp((w * np.where(known, log_gdp, 0)).sum(axis=1) / on_known)
        return np.where(on_known >= MIN_KNOWN * total, mean, np.nan)


by_production = destination_gdp(m["replaced_share"].astype(float))
by_impact = destination_gdp(m[f"{args.metric}_leaked"][i].astype(float))
production = np.nan_to_num(m["country_production_kg"])
ok = np.isfinite(own) & np.isfinite(by_production) & np.isfinite(by_impact)
print(f"Leaving out {(~ok).sum()} countries with no GDP per capita, or too little of their weight on places with one: "
      f"{', '.join(m['country_isos'][~ok])}")
isos, production = m["country_isos"][ok], production[ok]
sizes = 8 + 400 * np.sqrt(production / production.max())
missing = sorted(set(annotate) - set(ANNOTATE) - set(isos))
if missing:
    print(f"Not labelling {', '.join(missing)}: not among the countries plotted")
named = np.flatnonzero(np.isin(isos, annotate))
dollars = FuncFormatter(lambda v, _: f"${v / 1000:.3g}k")

name = pf.NAMES[args.metric]
panels = {
    "production": (own[ok], by_production[ok], "GDP per capita of the displacing country",
                   "Where the replacing production is", "Where the production goes"),
    "impact": (own[ok], by_impact[ok], "GDP per capita of the displacing country",
               f"Where the leaked {name} are" if args.metric == "bd" else f"Where the leaked {name} is",
               f"Where the {name} go" if args.metric == "bd" else f"Where the {name} goes"),
    "compare": (by_production[ok], by_impact[ok], "Where the replacing production is", "Where the leaked impact is",
                "Impact against production"),
}
chosen = [panel for panel in PANELS if panel in args.panels]
panels = [panels[panel] for panel in chosen]

# a 5-inch column per panel, under the title and note, wrapped to the width; in inches: the axes 3.942 tall, 0.54
# under them, and 0.918 over them for a line each of title and note, plus a line height for each line more
width = 5 * len(panels)
title_lines = textwrap.wrap(f"{c.item_label(m)}: does leakage move to poorer countries? Displacing "
                            f"{pf.size_text(m, m['sizes'][i])}", int(width / 0.09))
note_lines = textwrap.wrap("GDP per capita, PPP (constant 2021 $), in 2021. The places abroad are averaged "
                           "geometrically, weighted by what each replaces or takes; the size of a point is its "
                           "production. Below the dashed y = x line, it goes to poorer places; the solid line is the least-squares "
                           "fit on the log scales.", int(width / 0.065))
title_extra = (len(title_lines) - 1) * 12 * 1.2 / 72  # 12-point lines, at matplotlib's 1.2 spacing
note_extra = (len(note_lines) - 1) * 8.5 * 1.3 / 72  # 8.5-point lines, at 1.3
header = 0.918 + title_extra + note_extra
height = 3.942 + 0.54 + header
fig, axes = plt.subplots(1, len(panels), figsize=(width, height), facecolor=pf.SURFACE, squeeze=False)
axes = axes[0]
lo = min(np.nanmin(v) for p in panels for v in p[:2]) * 0.8
hi = max(np.nanmax(v) for p in panels for v in p[:2]) * 1.25
for ax, (x, y, x_label, y_label, title) in zip(axes, panels):
    c.new_axes(ax)
    ax.plot([lo, hi], [lo, hi], color=pf.INK_2, linewidth=1, linestyle=(0, (4, 3)), zorder=2)
    ax.scatter(x, y, s=sizes, color=pf.SERIES, edgecolor=pf.SURFACE, linewidth=1, alpha=0.75, zorder=3)
    # the line of best fit: least squares of log y on log x, a straight line on the log axes, over the countries' range
    slope, intercept = np.polyfit(np.log(x), np.log(y), 1)
    fit_x = np.array([x.min(), x.max()])
    ax.plot(fit_x, np.exp(intercept) * fit_x ** slope, color=pf.INK, linewidth=1.25, zorder=4)
    # label the largest producers and those furthest from y = x, every country, none, or those named
    if annotate == ["furthest"]:
        distance = np.abs(np.log(y / x))
        labelled = set(np.argsort(-production)[:8]) | set(np.argsort(-distance)[:6])
    else:
        labelled = range(len(x)) if annotate == ["all"] else [] if annotate == ["none"] else named
    for j in labelled:
        ax.annotate(isos[j], (x[j], y[j]), xytext=(4, 3), textcoords="offset points", fontsize=7.5, color=pf.INK)
    below = y < x
    ax.text(0.98, 0.02, f"below y = x: {below.sum()} of {len(x)} countries,\n{production[below].sum() / production.sum():.0%} "
            f"of their production\nbest fit: slope {slope:.2f}", transform=ax.transAxes, ha="right", va="bottom", fontsize=7.5, color=pf.INK_2)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlim(lo, hi)
    ax.set_ylim(lo, hi)
    ax.xaxis.set_major_formatter(dollars)
    ax.yaxis.set_major_formatter(dollars)
    ax.xaxis.set_minor_formatter(FuncFormatter(lambda *_: ""))
    ax.yaxis.set_minor_formatter(FuncFormatter(lambda *_: ""))
    ax.set_xlabel(x_label, color=pf.INK_2, fontsize=8.5)
    ax.set_ylabel(y_label, color=pf.INK_2, fontsize=8.5)
    ax.set_title(title, color=pf.INK, fontsize=10, loc="left")
fig.suptitle("\n".join(title_lines), x=0.15 / width, y=1 - 0.135 / height, ha="left", va="top", color=pf.INK,
             fontsize=12)
fig.text(0.15 / width, 1 - (0.54 + title_extra + note_extra) / height, "\n".join(note_lines),
         va="baseline", color=pf.INK_2, fontsize=8.5, linespacing=1.3)  # at its last line's baseline
fig.subplots_adjust(left=0.75 / width, right=1 - 0.15 / width, top=1 - header / height, bottom=0.54 / height,
                    wspace=0.22)
which = "" if chosen == PANELS else "_" + "_".join(chosen)  # the three panels keep the plain name
c.save(fig, f"destination_gdp_{c.stem(m)}_{args.metric}_size{m['sizes'][i]:.3g}{which}.png", args.config)
