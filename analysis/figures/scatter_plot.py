# -*- coding: utf-8 -*-
"""
Displacement against replacement, one point per displacing country C, for one
item, metric and displacement size, from the run's flow matrices:
`python scatter_plot.py ITEM [--axes A] [--size S] [--metric M] [--total] [--min-share F] [--annotate N]
[--uncertainty] [--symlog | --log10] [--config CONFIG]`, with ITEM an item code or name, or portfolio. --axes is one of:

    flows (the default)
      x  spared: every change the displacement makes, wherever the land is (C's
         own, and for animal products their feed's, where it was grown), the
         spared matrix's row sum; positive where land use, or impact, falls
      y  leaked: every change the replacement makes, wherever the land is, C
         included (the part of its production C replaces itself, and feed grown
         in C for the other producers), the per_kg matrix's row sum
      so y / x is C's leakage ratio, and the dashed y = x line is a ratio of 1,
      no net change: points above it have net leakage.

    domestic
      x  net domestic change: leaked to C - spared in C, the C column of the net
         matrix (negative where C's land use, or impact, falls)
      y  net leakage: leaked - spared over every location, C included, the net
         matrix's row sum (the run's {metric}_net)
      As y = x + the net change abroad, the dashed y = x line is no net change
      abroad: points above it have net leakage abroad.

All per kg displaced in C, or with --total for the whole displacement at that
size. --min-share F leaves out the countries producing less than the share F of
the item's world production (the total over the run's countries; for the
portfolio, of every item's), e.g. 0.0001 for 0.01%. --log10 leaves out the
countries where x or y isn't positive. With --axes
domestic it flips x to the net domestic reduction (-x), so all the countries left
have net leakage abroad, and their y = x line is off the axes.

--uncertainty adds bars over the run's Monte Carlo draws of the betas, from its
lowest to its highest percentile (5th and 95th) of what C spares and leaks in
all, which are those totals' own percentiles; the points stay at the central
betas. It's for --axes flows and the extensified metrics only, as the draws give
no percentiles of the net change in C, production GHG has no draws, and the
portfolio has no percentiles. On --log10 axes, bars that reach 0 or below run off
the axes. Written to
{figs_dir}/{run_name}/, with figs_dir {results_dir}/figures where it is null.
"""

import numpy as np

import _plot_funcs as pf
import flow_matrices as fm  # on the path from _plot_funcs

parser = pf.parser(__doc__)
parser.add_argument("--size", type=float, default=None,
                    help="the displacement size; the run's nearest is used (default: the smallest, the marginal case)")
parser.add_argument("--total", action="store_true", help="the whole displacement at that size, not per kg displaced")
args = parser.parse_args()

m = pf.load(args)
i = fm.size_index(m, args.size)
x, y = (values[i] for values in pf.axis_values(m, args))
ranges = []  # x_low, x_high, y_low and y_high, with --uncertainty
if args.uncertainty:
    *ranges, mc = pf.mc_ranges(m, args)
    ranges = [values[i] for values in ranges]
if args.total:
    x, y = x * m["displaced_kg"][i], y * m["displaced_kg"][i]
    ranges = [values * m["displaced_kg"][i] for values in ranges]
keep = pf.keep(m, args, x[None], y[None])
x, y, countries = x[keep], y[keep], m["country_isos"][keep]
ranges = [values[keep] for values in ranges]

size = m["sizes"][i]
values = np.concatenate([x, y, *ranges])
fig, ax = pf.new_figure(args, values)
if args.uncertainty:
    pf.draw_ranges(ax, args, x, y, *ranges)
ax.scatter(x, y, s=36, color=pf.SERIES, edgecolor=pf.SURFACE, linewidth=1, alpha=0.9, zorder=3)
pf.finish(ax, args, x, y, countries, values, pf.UNITS[args.metric] + ("" if args.total else " per kg displaced"),
          f"{m['item_name'].item()}: {pf.NAMES[args.metric]}, displacing {pf.size_text(m, size)}",
          key=f"bars: {mc}" if args.uncertainty else None)
pf.save(fig, args, m, "scatter",
        f"_size{size:.3g}" + ("_total" if args.total else "") + ("_mc" if args.uncertainty else ""), len(x))
