# -*- coding: utf-8 -*-
"""
The leakage ratio against GDP per capita and self-sufficiency:
`python leakage_vs_axes.py [--metric M] [--item ITEM] [--size S] [--min-share F] [--config CONFIG]`
(default: extinctions, the production portfolio, size 1, countries
with at least 0.01% of the world's production). One point per displacing country
C, sized by its production:

  left   GDP per capita (x) against C's leakage ratio, all leaked ÷ all spared
         (y), coloured by self-sufficiency
  right  self-sufficiency (x) against the leakage ratio, coloured by GDP per capita

Self-sufficiency is the share of C's consumption produced in C, from the FLMP
trade matrix (see country_attributes.py). The ratio is on log axes, so countries
that spare nothing or less (2 of them, at size 1, for the portfolio's
extinctions) and those with a ratio of 0 or less are left out; those beyond
--y-range are drawn at its edge as triangles. Small producers have the most
extreme ratios, as they tend to spare little, which --min-share leaves out.
"""

import argparse
from pathlib import Path

import numpy as np
from matplotlib.colors import LinearSegmentedColormap, LogNorm, Normalize
from matplotlib.ticker import FuncFormatter

import _common as c

pf, fm, plt = c.pf, c.fm, c.plt
# the blue ramp from step 250, so the lightest points still stand off the surface
MARKS = LinearSegmentedColormap.from_list("blue_marks", ["#86b6ef", "#3987e5", "#1c5cab", "#0d366b"])

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--metric", choices=fm.EXTENSIFIED, default="bd", help="(default: bd)")
parser.add_argument("--item", default=fm.PORTFOLIO, help="an item code or name, or portfolio (default)")
parser.add_argument("--size", type=float, default=1, help="the displacement size, the run's nearest (default: 1)")
parser.add_argument("--min-share", type=float, default=1e-4,
                    help="leave out the countries producing less than this share of the world's production "
                         "(default: 1e-4, 0.01%%)")
parser.add_argument("--y-range", type=float, nargs=2, default=[0.25, 8], help="the ratio axis (default: 0.25 8)")
parser.add_argument("--config", type=Path, default=c._run_paths.DEFAULT_CONFIG, help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()

m = c.load(args.item, args.config)
i = fm.size_index(m, args.size)
a = c.attributes().reindex(m["country_isos"])
production = np.nan_to_num(m["country_production_kg"])
ratio, spared = m[f"{args.metric}_ratio"][i], m[f"{args.metric}_spared_total"][i]
gdp, self_sufficiency = a["gdp_pc"].to_numpy(), a["self_sufficiency"].to_numpy()

big = production >= args.min_share * production.sum()
on_axes = (spared > 0) & (ratio > 0)
known = np.isfinite(gdp) & np.isfinite(self_sufficiency)
print(f"Leaving out {(~big).sum()} countries producing less than {args.min_share:.3g} of the world's production, "
      f"then {(big & ~on_axes).sum()} sparing nothing or less, or with a ratio of 0 or less "
      f"({', '.join(m['country_isos'][big & ~on_axes])}), and {(big & on_axes & ~known).sum()} with no GDP per capita "
      f"or self-sufficiency ({', '.join(m['country_isos'][big & on_axes & ~known])})")
ok = big & on_axes & known
isos, production, ratio, gdp, self_sufficiency = (v[ok] for v in (m["country_isos"], production, ratio, gdp,
                                                                  self_sufficiency))
sizes = 10 + 400 * np.sqrt(production / production.max())
low, high = args.y_range
clipped = np.clip(ratio, low, high)
outside = (ratio < low) | (ratio > high)
if outside.any():
    print(f"Beyond the ratio axis, at its edge: " + ", ".join(f"{iso} {r:.3g}" for iso, r in zip(isos[outside],
                                                                                                ratio[outside])))


def spearman(x: np.ndarray, y: np.ndarray) -> float:
    rx, ry = (np.argsort(np.argsort(v)) for v in (x, y))
    return float(np.corrcoef(rx, ry)[0, 1])


panels = [
    (gdp, "GDP per capita, PPP (constant 2021 $)", self_sufficiency, Normalize(0, 1), "Self-sufficiency",
     FuncFormatter(lambda v, _: f"{v:.0%}"), True),
    (self_sufficiency, "Self-sufficiency: share of consumption produced at home", gdp,
     LogNorm(np.nanmin(gdp), np.nanmax(gdp)), "GDP per capita", FuncFormatter(lambda v, _: f"${v / 1000:.3g}k"),
     False),
]
fig = plt.figure(figsize=(13, 5.6), facecolor=pf.SURFACE)
grid = fig.add_gridspec(1, 5, width_ratios=[1, 0.025, 0.12, 1, 0.025], wspace=0.05, left=0.06, right=0.93, top=0.84,
                        bottom=0.1)
axes = [fig.add_subplot(grid[0, 0])]
axes.append(fig.add_subplot(grid[0, 3], sharey=axes[0]))
colour_axes = [fig.add_subplot(grid[0, 1]), fig.add_subplot(grid[0, 4])]
for ax, cax, (x, x_label, colour, norm, colour_label, colour_format, log_x) in zip(axes, colour_axes, panels):
    c.new_axes(ax)
    ax.axhline(1, color=pf.INK_2, linewidth=1, linestyle=(0, (4, 3)), zorder=2)
    order = np.argsort(-sizes)  # the large points behind the small ones
    points = ax.scatter(x[order][~outside[order]], clipped[order][~outside[order]], s=sizes[order][~outside[order]],
                        c=colour[order][~outside[order]], cmap=MARKS, norm=norm, edgecolor=pf.SURFACE, linewidth=1,
                        zorder=3)
    ax.scatter(x[outside], clipped[outside], s=40, c=colour[outside], cmap=MARKS, norm=norm,
               marker=np.where(ratio[outside] > high, "^", "v")[0] if outside.any() else "^", edgecolor=pf.INK_2,
               linewidth=0.8, zorder=3)
    for j in set(np.argsort(-production)[:8]) | set(np.argsort(-np.abs(np.log(ratio)))[:7]):
        ax.annotate(isos[j], (x[j], clipped[j]), xytext=(4, 3), textcoords="offset points", fontsize=7.5,
                    color=pf.INK)
    if log_x:
        ax.set_xscale("log")
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"${v / 1000:.3g}k"))
    else:
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_xlabel(x_label, color=pf.INK_2, fontsize=8.5)
    ax.text(0.98, 0.02, f"Spearman's ρ with the ratio: {spearman(x, ratio):+.2f}, {len(x)} countries",
            transform=ax.transAxes, ha="right", va="bottom", fontsize=7.5, color=pf.INK_2)
    bar = fig.colorbar(points, cax=cax, format=colour_format)
    bar.outline.set_visible(False)
    bar.ax.tick_params(which="both", length=0, labelsize=7, colors=pf.INK_2)
    bar.ax.yaxis.set_minor_formatter(FuncFormatter(lambda *_: ""))
    bar.set_label(colour_label, color=pf.INK_2, fontsize=8)
axes[0].set_yscale("log")
axes[0].set_ylim(low * 0.9, high * 1.1)
axes[0].yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
axes[0].yaxis.set_minor_formatter(FuncFormatter(lambda v, _: f"{v:g}" if v in (0.3, 0.5, 2, 3, 5) else ""))
axes[0].set_ylabel(f"Leakage ratio, {pf.NAMES[args.metric]}: all leaked ÷ all spared", color=pf.INK_2, fontsize=8.5)
fig.suptitle(f"{c.item_label(m)}: leakage ratio against GDP and self-sufficiency, displacing "
             f"{pf.size_text(m, m['sizes'][i])}", x=0.01, y=0.975, ha="left", color=pf.INK, fontsize=12)
fig.text(0.01, 0.905, f"Countries with at least {args.min_share:.2%} of the world's production; the size of a point is "
         f"its production. Above the dashed line, the replacement leaks more than the displacement spares. Triangles: "
         f"beyond the axis.", color=pf.INK_2, fontsize=8.5)
c.save(fig, f"leakage_vs_axes_{c.stem(m)}_{args.metric}_size{m['sizes'][i]:.3g}.png", args.config,
       folder=c.VS_ATTRIBUTES)
