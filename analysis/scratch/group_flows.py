# -*- coding: utf-8 -*-
"""
Leakage between groups of countries, World Bank income groups or regions, at one
displacement size: `python group_flows.py [--groups income|region] [--metric M] [--item ITEM] [--size S]
[--config CONFIG]` (default: income groups, extinctions, the production portfolio, the smallest size).

Each country's row is weighted by its production, as if every country displaced
the same share of its production, each in its own scenario, and the rows and
columns summed within each group: G[A, B] is what A's countries' displacements
leak to (or spare in) B's. Countries with no group (no World Bank income group,
such as Venezuela's, or not in the World Bank's list, such as Taiwan) are left
out, and the share of the impact that leaves out is printed. Three panels, as in
_common.flow_panels: where A's leakage lands, the net change in B as % of all A
spares, and the net direction between A and B as % of all that's spared worldwide.
"""

import argparse
from pathlib import Path

import numpy as np

import _common as c

pf, fm, plt = c.pf, c.fm, c.plt

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--groups", choices=list(c.GROUPINGS), default="income", help="(default: income)")
parser.add_argument("--metric", choices=fm.METRICS, default="bd", help="(default: bd)")
parser.add_argument("--item", default=fm.PORTFOLIO, help="an item code or name, or portfolio (default)")
parser.add_argument("--size", type=float, default=None, help="the displacement size (default: the smallest)")
parser.add_argument("--config", type=Path, default=c._run_paths.DEFAULT_CONFIG, help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()

m = c.load(args.item, args.config)
i = fm.size_index(m, args.size)
column, groups = c.GROUPINGS[args.groups]
group_of = c.attributes()[column]
values = c.flow_panels(*c.flows(m, args.metric, i, group_of.dropna().to_dict(), groups))
panels = [(*c.FLOW_PANELS[key], values[key], cmap, diverging) for key, cmap, diverging in
          [("lands", c.BLUES, False), ("net", c.DIVERGING, True), ("direction", c.DIVERGING, True)]]

labels = [c.GROUP_LABELS.get(g, g) for g in groups]
n = len(groups)
fig, axes = plt.subplots(1, 3, figsize=(4.6 * 3 + 1.5, 0.62 * n + 2.6), facecolor=pf.SURFACE)
for ax, (title, unit, values, cmap, diverging) in zip(axes, panels):
    unit = unit.format(unit="group")
    ax.set_facecolor(pf.SURFACE)
    for spine in ax.spines.values():
        spine.set_visible(False)
    finite = values[np.isfinite(values)]
    limit = np.abs(finite).max() if diverging else finite.max()
    image = ax.imshow(values, cmap=cmap, vmin=-limit if diverging else 0, vmax=limit, aspect="auto")
    # the 2px surface gap between cells
    ax.set_xticks(np.arange(n + 1) - 0.5, minor=True)
    ax.set_yticks(np.arange(n + 1) - 0.5, minor=True)
    ax.grid(which="minor", color=pf.SURFACE, linewidth=2)
    ax.tick_params(which="both", length=0, colors=pf.INK, labelsize=8)
    ax.set_xticks(range(n), labels, rotation=30 if n > 4 else 0, ha="right" if n > 4 else "center")
    ax.set_yticks(range(n), labels if ax is axes[0] else [])
    for a in range(n):
        for b in range(n):
            v = values[a, b]
            if np.isfinite(v):
                dark = abs(v) / limit > 0.55
                ax.text(b, a, f"{v * 100:+.1f}%" if diverging else f"{v * 100:.1f}%", ha="center", va="center",
                        fontsize=8 if n <= 4 else 7, color="#ffffff" if dark else pf.INK)
    ax.set_title(title, color=pf.INK, fontsize=10, loc="left")
    ax.set_xlabel("Where the impact lands (B)", color=pf.INK_2, fontsize=8.5)
    bar = fig.colorbar(image, ax=ax, fraction=0.045, pad=0.02)
    bar.outline.set_visible(False)
    bar.ax.tick_params(length=0, labelsize=7, colors=pf.INK_2)
    bar.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v * 100:.3g}%"))
    bar.set_label(unit, color=pf.INK_2, fontsize=8)
axes[0].set_ylabel("Displacing group (A)", color=pf.INK_2, fontsize=8.5)
size = pf.size_text(m, m["sizes"][i])
fig.suptitle(f"{c.item_label(m)}: {pf.NAMES[args.metric]} between World Bank {args.groups} groups, displacing {size}",
             x=0.01, y=0.985, ha="left", color=pf.INK, fontsize=12)
fig.text(0.01, 0.92 if n <= 4 else 0.935, "Every country displacing the same share of its production, in its own "
         "scenario, and summed within each group. Leakage includes what each country replaces itself.",
         color=pf.INK_2, fontsize=8.5)
fig.tight_layout(rect=(0, 0, 1, 0.9 if n <= 4 else 0.92))
c.save(fig, f"group_flows_{args.groups}_{c.stem(m)}_{args.metric}_size{m['sizes'][i]:.3g}.png", args.config)
