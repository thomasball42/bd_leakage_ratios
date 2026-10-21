# -*- coding: utf-8 -*-
"""
Net leakage against net domestic change, one point per displacing country C, for
one item, metric and displacement size, from the run's flow matrices:
`python scatter_plot.py ITEM [--size S] [--metric M] [--total] [--symlog] [--config CONFIG]`,
with ITEM an item code or name.

    x  net domestic change: leaked to C - spared in C, the C column of the net
       matrix (negative where C's land use, or impact, falls)
    y  net leakage: leaked - spared over every location, C included, the net
       matrix's row sum (the run's {metric}_net)

both per kg displaced in C, or with --total for the whole displacement at that
size. As y = x + the net change abroad, the dashed y = x line is no net change
abroad: points above it have net leakage abroad. Written to the run's
processed/figures/.
"""

import argparse
import sys
from pathlib import Path

import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # for the analysis modules
import _run_paths  # noqa: E402
import flow_matrices as fm  # noqa: E402

UNITS = {"land_use_m2": "m²", "bd": "extinctions", "ghg_coc_kgco2e": "kg CO₂e", "ghg_prod_kgco2e": "kg CO₂e"}
NAMES = {"land_use_m2": "land use", "bd": "extinctions", "ghg_coc_kgco2e": "carbon opportunity cost",
         "ghg_prod_kgco2e": "production GHG"}
N_LABELS = 8  # the countries labelled: the furthest from the origin
# the reference palette's light theme: surface, ink, recessive gray, and categorical slot 1
SURFACE, INK, INK_2, GRID, SERIES = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df", "#2a78d6"

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("item", help="an item code or name, as in the flow matrices' items.csv")
parser.add_argument("--size", type=float, default=None,
                    help="the displacement size; the run's nearest is used (default: the smallest, the marginal case)")
parser.add_argument("--metric", default="land_use_m2", choices=fm.METRICS, help="(default: land_use_m2)")
parser.add_argument("--total", action="store_true", help="the whole displacement at that size, not per kg displaced")
parser.add_argument("--symlog", action="store_true", help="symmetric log axes, for heavy tails")
parser.add_argument("--config", type=Path, default=_run_paths.DEFAULT_CONFIG,
                    help="the run's config.json, or its record (default: the repository's config.json)")
args = parser.parse_args()

m = fm.load(int(args.item) if args.item.isdigit() else args.item, args.config)
i = fm.size_index(m, args.size)
net = fm.cube(m, args.metric, "net")[i]
x = np.where(fm.self_cells(m), net, 0).sum(axis=1)  # each row's C column (0 where C has none)
y = np.where(np.isnan(net).all(axis=1), np.nan, np.nansum(net, axis=1))
if args.total:
    x, y = x * m["displaced_kg"][i], y * m["displaced_kg"][i]
countries = m["country_isos"]
ok = np.isfinite(x) & np.isfinite(y)
if (~ok).any():
    print(f"Leaving out {(~ok).sum()} countries with no extensified result: {', '.join(countries[~ok])}")
x, y, countries = x[ok], y[ok], countries[ok]

size = m["sizes"][i]
unit = UNITS[args.metric] + ("" if args.total else " per kg displaced")
size_text = f"{size:.3g} of production" if m["size_unit"].item() == "share" else f"{size:.3g} kg"

fig, ax = plt.subplots(figsize=(7, 6), facecolor=SURFACE)
ax.set_facecolor(SURFACE)
for spine in ax.spines.values():
    spine.set_visible(False)
ax.grid(color=GRID, linewidth=0.8)
ax.set_axisbelow(True)
ax.tick_params(colors=INK_2, length=0, labelsize=9)
ax.axhline(0, color=INK_2, linewidth=0.8, zorder=1)
ax.axvline(0, color=INK_2, linewidth=0.8, zorder=1)
if args.symlog:
    nonzero = np.abs(np.concatenate([x, y]))
    linthresh = np.percentile(nonzero[nonzero > 0], 10) if (nonzero > 0).any() else 1
    ax.set_xscale("symlog", linthresh=linthresh)
    ax.set_yscale("symlog", linthresh=linthresh)

ax.scatter(x, y, s=36, color=SERIES, edgecolor=SURFACE, linewidth=1, alpha=0.9, zorder=3)
lo, hi = min(x.min(), y.min()), max(x.max(), y.max())
ax.plot([lo, hi], [lo, hi], color=INK_2, linewidth=1, linestyle=(0, (4, 3)), zorder=2)
ax.text(0.99, 0.01, "dashed: y = x, no net change abroad", transform=ax.transAxes, ha="right", va="bottom",
        fontsize=8, color=INK_2)

# label the countries furthest from the origin, each axis scaled by its spread
spread = np.array([np.ptp(x) or 1, np.ptp(y) or 1])
furthest = np.argsort(-np.hypot(x / spread[0], y / spread[1]))[:N_LABELS]
for j in furthest:
    ax.annotate(countries[j], (x[j], y[j]), xytext=(5, 3), textcoords="offset points", fontsize=8, color=INK)

ax.set_xlabel(f"Net domestic change ({unit})", color=INK, fontsize=10)
ax.set_ylabel(f"Net leakage, domestic included ({unit})", color=INK, fontsize=10)
ax.set_title(f"{m['item_name'].item()}: {NAMES[args.metric]}, displacing {size_text}",
             color=INK, fontsize=11, loc="left")
fig.tight_layout()

out_dir = _run_paths.run_paths(args.config)[2] / "figures"
out_dir.mkdir(parents=True, exist_ok=True)
path = out_dir / (f"scatter_{m['item_code'].item()}_{args.metric}_size{size:.3g}"
                  + ("_total" if args.total else "") + ".png")
fig.savefig(path, dpi=200, facecolor=SURFACE)
print(f"{len(x)} countries; wrote {path}")
