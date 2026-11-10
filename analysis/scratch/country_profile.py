# -*- coding: utf-8 -*-
"""
Where the impacts of one country's displaced production go, at one displacement
size: `python country_profile.py [ISO ...] [--item ITEM] [--size S] [--top N] [--metrics M ...]
[--groups region|income] [--drop-ungrouped] [--uncertainty [--percentiles LO HI]]
[--global-net separate|together] [--config CONFIG]`
(default: GBR BRA, the production portfolio, size 1, land use, extinctions and carbon
opportunity cost, by country). Three figures per country C (two with --global-net together), with a
panel per metric, in the scratch figures' profiles/ folder:

  profile_{ISO}_{item}_bars.png
    For each metric, what's spared (left, blue) and leaked (right, orange) at
    each location, and the net change there (dot), in the metric's units per kg
    displaced (the flow matrices' own, at that size), so the spared bars sum to all
    C spares and the leaked bars to all it leaks. The top panels are C itself;
    below, on the same scale, are the N locations abroad with the largest net
    change in any metric (relative to all C spares in it, so the metrics can be
    compared), from the largest net increase to the largest net decrease, then the
    rest of the world. With --global-net together, the net global effect is at the
    bottom, set apart under a rule, on the same scale.

  profile_{ISO}_{item}_neteffect.png  (not with --global-net together)
    The net global effect: for each metric, all C spares and all it leaks, summed
    over every location (any left out by --drop-ungrouped too), and the net change,
    written above it too, also as a share of all C spares (the leakage ratio less
    1). It's the bars' width, with their labels' column, panels, fonts and bars'
    height, and the same x10^n, so the two can be stacked on a slide.

  profile_{ISO}_{item}_shares.png
    For each metric, one point per location: its share of the production that
    replaces C's (x) against its share of the impact that leaks (y), C included.
    Locations above the dashed y = x line take more of the impact than of the
    production. Locations that only grow feed for the replacing animals have no
    share of the production, and aren't shown; the note says how much of the
    impact they take.

With --groups, the locations abroad are summed into their World Bank regions or
income groups (C's own without C, and those with none into one more, or left out
with --drop-ungrouped), all shown unless --top is fewer. With --metrics other than
the default, or --groups, those are in the file names, e.g.
profile_GBR_portfolio_bd_by_region_bars.png.

With --uncertainty, each bar has whiskers from the 5th to the 95th percentile (or
--percentiles) over the run's Monte Carlo draws of what's spared or leaked there,
in the same units. They're from the draws the run
saves for each location (monte_carlo.save_draws), summed in each draw for the
groups and the rest of the world, so they're exact for any row. The run saves them
only for the portfolio and the items, at the sizes, in its save_draws. Production
GHG has no whiskers, as it doesn't depend on the betas.
"""

import argparse
import textwrap
from pathlib import Path

import numpy as np
from matplotlib.ticker import FuncFormatter
from matplotlib.transforms import Bbox, ScaledTranslation

import _common as c

pf, fm, plt = c.pf, c.fm, c.plt

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("countries", nargs="*", default=["GBR", "BRA"], help="ISO3 codes (default: GBR BRA)")
parser.add_argument("--item", default=fm.PORTFOLIO, help="an item code or name, or portfolio (default)")
parser.add_argument("--size", type=float, default=1, help="the displacement size, the run's nearest (default: 1)")
parser.add_argument("--top", type=int, default=12,
                    help="the locations abroad shown in the bars, or groups with --groups (default: 12)")
parser.add_argument("--metrics", nargs="+", choices=fm.METRICS, default=c.METRICS, metavar="M",
                    help=f"the metrics plotted, a panel each, from {', '.join(fm.METRICS)} "
                         f"(default: {' '.join(c.METRICS)})")
parser.add_argument("--groups", choices=list(c.GROUPINGS), default=None,
                    help="sum the locations abroad into World Bank regions or income groups (default: by country)")
parser.add_argument("--drop-ungrouped", action="store_true",
                    help="with --groups, leave out the locations with no group (the 'No region' row); the share of "
                         "the leakage that leaves out is printed")
parser.add_argument("--uncertainty", action="store_true",
                    help="whiskers on the bars, over the run's Monte Carlo draws (needs the draws saved for the item "
                         "and size: the run's monte_carlo.save_draws)")
parser.add_argument("--percentiles", nargs=2, type=float, default=[5, 95], metavar=("LO", "HI"),
                    help="the whiskers' percentiles, with --uncertainty (default: 5 95)")
parser.add_argument("--global-net", choices=["separate", "together"], default="separate",
                    help="the net global effect (all spared and all leaked, summed over every location) in its own "
                         "figure, *_neteffect.png, lined up with the bars (separate, the default), or at the bottom "
                         "of the bars (together)")
parser.add_argument("--floor", type=float, default=1e-4,
                    help="the smallest share shown on the shares' log axes (default: 1e-4, 0.01%%)")
parser.add_argument("--config", type=Path, default=c._run_paths.DEFAULT_CONFIG, help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()
if args.drop_ungrouped and not args.groups:
    parser.error("--drop-ungrouped needs --groups")
if not 0 <= args.percentiles[0] < args.percentiles[1] <= 100:
    parser.error(f"--percentiles must be LO HI with 0 <= LO < HI <= 100, not {args.percentiles}")

metrics =list(dict.fromkeys(args.metrics))
together = args.global_net == "together"
suffix = ("" if metrics == c.METRICS else "_" + "_".join(metrics)) + (f"_by_{args.groups}" if args.groups else "")
m = c.load(args.item, args.config)
i = fm.size_index(m, args.size)
attributes = c.attributes()
names = attributes["name"]
isos, locations = list(m["country_isos"]), m["location_isos"]
percent = FuncFormatter(lambda v, _: f"{v * 100:.3g}%")
# where a point's label can go, in order of preference: beside it, then at its corners, then above and below it
# (offset in points, and alignment)
PLACES = [(5, 0, "left", "center"), (-5, 0, "right", "center"), (4, 3, "left", "bottom"), (-4, 3, "right", "bottom"),
          (4, -3, "left", "top"), (-4, -3, "right", "top"), (0, 6, "center", "bottom"), (0, -6, "center", "top")]
if args.groups:
    column, groups = c.GROUPINGS[args.groups]
    group_of = attributes[column]
    noun = {"region": "region", "income": "income group"}[args.groups]


def name(iso: str, width: int = 24) -> str:
    """The World Bank's name for a location, cut to `width` characters, or its code (or group) where it has none."""
    full = names.get(iso)
    full = full if isinstance(full, str) else iso
    return full if len(full) <= width else full[:width - 1].rstrip(" ,.") + "…"


def grouped(iso: str, *values: dict | np.ndarray, drop: bool = False) -> tuple[list, np.ndarray, int]:
    """`values`, each an array over the locations or a dict of them, summed into C (`iso`), the groups of the
    locations abroad (C's own without C) and one for those with none, unless `drop`, which leaves them out; those
    units' labels, C's its ISO3; and how many locations are left out."""
    none = f"No {noun}"
    of = [iso if loc == iso else g if g in groups else none for loc, g in zip(locations, group_of.reindex(locations))]
    units = [u for u in [iso, *groups, *([] if drop else [none])] if u in of]
    at = np.array([units.index(u) if u in units else -1 for u in of])
    kept = at >= 0

    def summed(v: np.ndarray) -> np.ndarray:
        out = np.zeros((len(units), *v.shape[1:]))
        np.add.at(out, at[kept], v[kept])
        return out

    labels = np.array([f"{u} ({of.count(u)})" if u == none else c.GROUP_LABELS.get(u, u) if len(u) > 24 else u
                       for u in units])
    return ([{k: summed(v) for k, v in x.items()} if isinstance(x, dict) else summed(x) for x in values], labels,
            int((~kept).sum()))


def label(ax: plt.Axes, points: list[tuple[float, float, str | None]]) -> None:
    """Label the (x, y, text) points that have a text, in turn, each at the first of PLACES around it that stays in
    the panel and overlaps no other point or earlier label, or else at the first."""
    renderer = ax.figure.canvas.get_renderer()
    panel = ax.get_window_extent(renderer)
    r = 4 * ax.figure.dpi / 72  # a point's radius and a little, in pixels
    dots = [Bbox.from_bounds(px - r, py - r, 2 * r, 2 * r)
            for px, py in ax.transData.transform([(x, y) for x, y, _ in points])]

    def note(text: str, x: float, y: float, dx: int, dy: int, ha: str, va: str) -> plt.Annotation:
        return ax.annotate(text, (x, y), xytext=(dx, dy), textcoords="offset points", ha=ha, va=va, fontsize=7.5,
                           color=pf.INK)

    labels = []
    for j, (x, y, text) in enumerate(points):
        if text is None:
            continue
        taken = labels + dots[:j] + dots[j + 1:]
        for place in PLACES:
            annotation = note(text, x, y, *place)
            box = annotation.get_window_extent(renderer)
            if panel.x0 <= box.x0 and box.x1 <= panel.x1 and not any(box.overlaps(other) for other in taken):
                break
            annotation.remove()
        else:
            box = note(text, x, y, *PLACES[0]).get_window_extent(renderer)
        labels.append(box)


def heading(fig: plt.Figure, left: float, title: str, subtitle: str) -> float:
    """Write the title and subtitle at the top of `fig`, `left` inches in, wrapped to its width at about half an em
    a character and a fixed distance from its top whatever its height, and return how far down they reach, in
    inches."""
    width = fig.get_figwidth() - left - 0.15
    y = 0.18
    for text, size, color, spacing in [(title, 12, pf.INK, 1.3), (subtitle, 8.5, pf.INK_2, 1.5)]:
        text = "\n".join(textwrap.fill(line, int(width * 72 / (0.55 * size))) for line in text.split("\n"))
        fig.text(left / fig.get_figwidth(), 1, text, color=color, fontsize=size, va="top", linespacing=spacing,
                 transform=fig.transFigure + ScaledTranslation(0, -y, fig.dpi_scale_trans))
        y += (text.count("\n") + 1) * size * spacing / 72 + 0.07
    return y - 0.07


def ordinal(q: float) -> str:
    """5 -> 5th, 2.5 -> 2.5th, 1 -> 1st."""
    text = f"{q:g}"
    return text + ({"1": "st", "2": "nd", "3": "rd"}.get(text[-1], "th") if text[-2:-1] != "1" else "th")


def bars(ax: plt.Axes, spared: np.ndarray, leaked: np.ndarray, labels: list[str],
         ranges: tuple[np.ndarray, np.ndarray] | None = None) -> None:
    """Spared to the left and leaked to the right of 0, with the net as a dot, one row per label, and whiskers
    from `ranges`' spared and leaked (each 2 x rows, low and high) where there are any."""
    y = np.arange(len(labels))
    ax.barh(y, -spared, height=0.62, color=c.SPARED, edgecolor=pf.SURFACE, linewidth=1)
    ax.barh(y, leaked, height=0.62, color=c.LEAKED, edgecolor=pf.SURFACE, linewidth=1)
    for (low, high), sign in zip(ranges or [], [-1, 1]):
        drawn = np.isfinite(low) & np.isfinite(high) & ((low != 0) | (high != 0))
        ends, rows = sign * np.concatenate([low[drawn], high[drawn]]), np.tile(y[drawn], 2)
        ax.hlines(y[drawn], sign * low[drawn], sign * high[drawn], color=pf.INK, linewidth=0.9, zorder=2.5)
        ax.vlines(ends, rows - 0.14, rows + 0.14, color=pf.INK, linewidth=0.9, zorder=2.5)
    ax.scatter(leaked - spared, y, s=22, color=pf.INK, edgecolor=pf.SURFACE, linewidth=1, zorder=3)
    ax.axvline(0, color=pf.INK_2, linewidth=0.8)
    ax.set_yticks(y, labels, fontsize=8, color=pf.INK)
    ax.set_ylim(len(labels) - 0.5, -0.5)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:+.4g}" if v else "0"))


def x_label(k: str, power: int) -> str:
    """The bars' x axis' label in metric k, over 10**power."""
    return f"{pf.UNITS[k]} per kg displaced" + (rf" ($\times10^{{{power}}}$)" if power else "")


def magnitude(*values: np.ndarray | tuple[np.ndarray, ...] | None) -> int:
    """The power of ten of the largest of `values`' magnitudes (arrays, tuples of them, or None), to show them in,
    or 0 where it's from 0.01 to 9,999, or there's none."""
    arrays = [np.abs(np.asarray(v, dtype=float)).ravel() for x in values if x is not None
              for v in (x if isinstance(x, tuple) else [x])]
    top = max([np.nanmax(a) for a in arrays if a.size and not np.isnan(a).all()], default=0)
    if not np.isfinite(top) or top <= 0:
        return 0
    power = int(np.floor(np.log10(top)))
    return 0 if -2 <= power <= 3 else power


def scaled(x: np.ndarray | tuple[np.ndarray, ...] | None, power: int) -> np.ndarray | tuple | None:
    """An array, or each of a tuple of them, over 10**power."""
    if x is None:
        return None
    return tuple(v / 10.0 ** power for v in x) if isinstance(x, tuple) else x / 10.0 ** power


for iso in args.countries:
    if iso not in isos:
        print(f"No {iso} in the flow matrices of {c.item_label(m)}")
        continue
    row = isos.index(iso)
    spared = {k: m[f"{k}_spared"][i, row].astype(float) for k in metrics}
    leaked = {k: m[f"{k}_leaked"][i, row].astype(float) for k in metrics}
    total = {k: m[f"{k}_spared_total"][i, row] for k in metrics}
    if not all(np.isfinite(t) and t > 0 for t in total.values()):
        print(f"{iso} spares nothing, or has no extensified result, in some metric: {total}; skipping")
        continue
    replaced = m["replaced_share"][row].astype(float)
    leaked_all = {k: leaked[k].sum() for k in metrics}  # with any locations left out by --drop-ungrouped
    # each location's spared and leaked in each draw (locations x draws), for the whiskers
    drawn = [k for k in metrics if k in fm.EXTENSIFIED] if args.uncertainty else []
    in_draws = fm.draws(m, iso, m["sizes"][i], args.config) if drawn else {}
    spared_draws = {k: in_draws[f"{k}_spared"] for k in drawn}
    leaked_draws = {k: in_draws[f"{k}_leaked"] for k in drawn}
    # the net global effect, over every location (before any are left out by --drop-ungrouped): all spared and all
    # leaked, and their whiskers, from their sums in each draw; and the net's range over the draws
    world = {k: [np.array([total[k]]), np.array([leaked_all[k]]),
                 tuple(np.percentile(x[k].sum(axis=0)[None], args.percentiles, axis=1)
                       for x in [spared_draws, leaked_draws]) if k in drawn else None] for k in metrics}
    net_range = {k: np.percentile((leaked_draws[k] - spared_draws[k]).sum(axis=0), args.percentiles) for k in drawn}
    span = f"{ordinal(args.percentiles[0])}–{ordinal(args.percentiles[1])} percentile"
    units, dropped = locations, 0
    if args.groups:
        (spared, leaked, replaced, spared_draws, leaked_draws), units, dropped = grouped(
            iso, spared, leaked, replaced, spared_draws, leaked_draws, drop=args.drop_ungrouped)
    if dropped:
        print(f"{iso}: leaving out the {dropped} locations with no {noun}, with "
              + ", ".join(f"{1 - leaked[k].sum() / leaked_all[k]:.3%} of the {c.SHORT[k].lower()} leaked"
                          for k in metrics))
    home = units == iso
    title = f"{name(iso)}, {c.item_label(m).lower()}, displacing {pf.size_text(m, m['sizes'][i])}"
    ratios = ", ".join(f"{c.SHORT[k].lower()} {leaked_all[k] / total[k]:.2f}" for k in metrics)
    several = len(metrics) > 1

    # the bars: C, then the locations (or groups) abroad with the largest net change in any metric (relative to all C
    # spares in it, so the metrics can be compared), from the largest increase to the largest decrease, then any rest
    nets = np.array([(leaked[k] - spared[k]) / total[k] for k in metrics])
    net = np.take_along_axis(nets, np.abs(nets).argmax(axis=0)[None], axis=0)[0]  # the largest, with its sign
    abroad = np.flatnonzero(~home)
    top = abroad[np.argsort(-np.abs(net[abroad]))][:args.top]
    top = top[np.argsort(-net[top])]
    rest = np.setdiff1d(abroad, top)
    rows = [[j] for j in top] + ([rest] if len(rest) else [])  # the units summed in each bar below C's

    def in_rows(values: np.ndarray, rows: list) -> np.ndarray:
        """`values` (units, or units x draws) summed over each of `rows`' units."""
        return np.array([values[r].sum(axis=0) for r in rows])

    def ranges(k: str, rows: list) -> tuple[np.ndarray, np.ndarray] | None:
        """The whiskers' spared and leaked ends (each 2 x rows) in metric k, per kg displaced."""
        return (tuple(np.percentile(in_rows(x[k], rows), args.percentiles, axis=1)
                      for x in [spared_draws, leaked_draws]) if k in drawn else None)

    def world_axes(ax: plt.Axes, k: str, power: int, first: bool) -> tuple[plt.Axes, plt.Annotation]:
        """The net global effect in metric k on `ax`, over 10**power, with the net (and its range over the draws, on
        a second line) written over it, and the note that says so."""
        c.new_axes(ax, grid="x")
        bars(ax, *[scaled(x, power) for x in world[k][:2]], ["World (all locations)"], scaled(world[k][2], power))
        times = rf"$\times10^{{{power}}}$" if power else ""
        net = world[k][1][0] - world[k][0][0]
        text = f"Net {net / 10.0 ** power:+.3g}{times}, {net / world[k][0][0]:+.1%} of all spared"
        if k in drawn:
            low, high = net_range[k] / 10.0 ** power
            text += f"\n{span}: {low:+.3g}{times} to {high:+.3g}{times}"
        note = ax.annotate(text, (0, 1), xycoords="axes fraction", xytext=(0, 4), textcoords="offset points",
                           ha="left", va="bottom", color=pf.INK, fontsize=8.5, linespacing=1.4)
        ax.set_xlabel(x_label(k, power), color=pf.INK_2, fontsize=8)
        if not first:
            ax.set_yticklabels([])
        return ax, note

    labels = [name(unit) for unit in units[top]] + ([f"Rest of world ({len(rest)})"] if len(rest) else [])
    which = (f"the {len(top)} locations abroad" if not args.groups
             else f"the {f'{len(top)} ' if len(rest) else ''}World Bank {noun}s abroad ({name(iso)}'s own without it)")
    any_metric = " in any metric" if several else ""
    order = (f" with the largest net change{any_metric}, from the largest net increase to the largest net decrease"
             if len(rest) else f", from the largest net increase to the largest net decrease{any_metric}")
    # 1.8 inches for the labels and 3.4 for each panel, wide enough for the legend
    width = max(1.8 + 3.4 * len(metrics), 7.5)
    fig = plt.figure(figsize=(width, 1), facecolor=pf.SURFACE)
    head = heading(fig, 1.8, title,
                   f"Home (top) and {which}{order}, in {'each metric' if several else 'the metric'}'s units per kg displaced."
                   + (" At the bottom, the net global effect: all of it, summed over every location." if together
                      else "")
                   + f"\nLeakage ratio, all leaked ÷ all spared: {ratios}")
    # the legend under the heading, on one row if it fits beside the labels' column, else on two
    handles = [plt.Rectangle((0, 0), 1, 1, color=c.SPARED), plt.Rectangle((0, 0), 1, 1, color=c.LEAKED),
               plt.Line2D([], [], marker="o", linestyle="", color=pf.INK, markersize=5)]
    entries = ["Spared by the displacement", "Leaked by the replacement", "Net change"]
    if drawn:
        handles.append(plt.Line2D([], [], color=pf.INK, linewidth=0.9, marker="|", markersize=7))
        entries.append(span)
    legend = dict(handles=handles, labels=entries, loc="upper left", frameon=False, fontsize=8, labelcolor=pf.INK)
    one_row = fig.legend(ncol=len(entries), **legend)
    fits = one_row.get_window_extent(fig.canvas.get_renderer()).width / fig.dpi <= width - 1.84
    one_row.remove()
    second_row = 0 if fits else 0.17
    # one bar's row, in inches (the 5.18 of the panels, less the gap between them, 6% of their height, over their
    # rows), as tall as the net global effect's panels; together, those are `gap` below the panels above, for their
    # tick labels, a rule, the section's heading and the net (on two lines with the whiskers' range)
    row = 5.18 / ((len(labels) + 1.5) * 1.06)
    gap = 0.92 + (0.14 if drawn else 0)
    above = 0.58 + (row + gap if together else 0)  # the bottom of the panels above, in inches from the figure's
    height = head + 6.38 + second_row + above - 0.58
    fig.set_figheight(height)
    grid = fig.add_gridspec(2, len(metrics), height_ratios=[1, len(labels) + 0.5], hspace=0.12, wspace=0.08,
                            left=1.8 / width, right=1 - 0.24 / width, top=1 - (head + 0.62 + second_row) / height,
                            bottom=above / height)
    if together:
        world_grid = fig.add_gridspec(1, len(metrics), wspace=0.08, left=1.8 / width, right=1 - 0.24 / width,
                                      top=(0.58 + row) / height, bottom=0.58 / height)
        fig.add_artist(plt.Line2D([0.15 / width, 1 - 0.24 / width], [(above - 0.34) / height] * 2,
                                  transform=fig.transFigure, color=pf.INK_2, linewidth=0.8))
        fig.text(0.15 / width, (above - 0.42) / height, "Net global effect", color=pf.INK, fontsize=10.5,
                 fontweight="bold", va="top")
    powers = {}  # each metric's, for the net global effect's figure too
    for j, k in enumerate(metrics):
        home_bars = [spared[k][home], leaked[k][home], ranges(k, [np.flatnonzero(home)])]
        row_bars = [in_rows(spared[k], rows), in_rows(leaked[k], rows), ranges(k, rows)]
        # the panel's, so its tick labels are short, and the net global effect's the same
        powers[k] = power = magnitude(*home_bars, *row_bars, *world[k])
        ax_home = c.new_axes(fig.add_subplot(grid[0, j]), grid="x")
        bars(ax_home, *[scaled(x, power) for x in home_bars[:2]], [f"{name(iso)} (home)"],
             scaled(home_bars[2], power))
        ax_home.set_title(c.SHORT[k], color=pf.INK, fontsize=10, loc="left")
        # on the home bar's scale, so they can be compared
        ax = c.new_axes(fig.add_subplot(grid[1, j], sharex=ax_home), grid="x")
        bars(ax, *[scaled(x, power) for x in row_bars[:2]], labels, scaled(row_bars[2], power))
        if len(rest):
            ax.axhline(len(labels) - 1.5, color=pf.GRID, linewidth=0.8)
        if j:
            ax_home.set_yticklabels([])
            ax.set_yticklabels([])
        if together:  # on the same scale, under the rule, with the axis' label
            world_axes(fig.add_subplot(world_grid[0, j], sharex=ax_home), k, power, first=not j)
        else:
            ax.set_xlabel(x_label(k, power), color=pf.INK_2, fontsize=8)
    fig.legend(ncol=len(entries) if fits else 2, bbox_to_anchor=(1.74 / width, 1 - head / height), **legend)
    c.save(fig, f"profile_{iso}_{c.stem(m)}{suffix}_bars.png", args.config, folder=c.PROFILES)

    if not together:
        # the net global effect on its own, lined up with the bars: their width, labels' column, panels, fonts and
        # bars' height, so the two can be stacked on a slide
        fig = plt.figure(figsize=(width, 1), facecolor=pf.SURFACE)
        head = heading(fig, 1.8, f"{title}: net global effect",
                       f"All {name(iso)} spares (left) and all it leaks (right), summed over every location, and the "
                       f"net change (dot), in {'each metric' if several else 'the metric'}'s units per kg displaced"
                       + (f", with whiskers from the {span} over the draws." if drawn else "."))
        # above the panels, the metric's name over the net (on two lines with the whiskers' range), in points
        titles = 10 * 1.2 + 7 + 8.5 * (1.2 + (1.4 if drawn else 0))
        margin = head + 0.15 + titles / 72  # over the panels, in inches
        height = margin + row + 0.58
        fig.set_figheight(height)
        grid = fig.add_gridspec(1, len(metrics), wspace=0.08, left=1.8 / width, right=1 - 0.24 / width,
                                top=1 - margin / height, bottom=0.58 / height)
        renderer = fig.canvas.get_renderer()
        for j, k in enumerate(metrics):
            ax, note = world_axes(fig.add_subplot(grid[0, j]), k, powers[k], first=not j)
            # just over the net, however tall it is
            ax.set_title(c.SHORT[k], color=pf.INK, fontsize=10, loc="left",
                         pad=7 + note.get_window_extent(renderer).height * 72 / fig.dpi)
        c.save(fig, f"profile_{iso}_{c.stem(m)}{suffix}_neteffect.png", args.config, folder=c.PROFILES)

    # the shares: of the production that replaces C's, against of the impact that leaks, per location (or group)
    unit_noun = noun if args.groups else "location"
    # 0.78 inches for the axis labels and 4.1 for each panel
    width = max(0.7 + 4.1 * len(metrics), 6)
    fig = plt.figure(figsize=(width, 1), facecolor=pf.SURFACE)
    head = heading(fig, 0.78, f"{title}: where the production goes against where the impact goes",
                   (f"One point per World Bank {noun} abroad ({name(iso)}'s own without it), and "
                    f"{name(iso)} in orange." if args.groups else f"One point per location, {name(iso)} in orange.")
                   + f" Above the dashed y = x line, {'an' if unit_noun[0] in 'aeiou' else 'a'} {unit_noun} takes "
                   "more of the impact than of the production.")
    height = head + 4.27
    fig.set_figheight(height)
    axes = fig.subplots(1, len(metrics), sharex=True, sharey=True, squeeze=False)[0]
    # laid out before the labels, which are placed where they fit
    fig.subplots_adjust(left=0.78 / width, right=1 - 0.13 / width, top=1 - (head + 0.3) / height,
                        bottom=0.54 / height, wspace=0.06)
    for ax, k in zip(axes, metrics):
        c.new_axes(ax)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlim(args.floor * 0.7, 1.4)
        ax.set_ylim(args.floor * 0.7, 1.4)
        share = leaked[k] / leaked_all[k]
        shown = (replaced >= args.floor) & (share >= args.floor)
        feed_only = (replaced <= 0) & (share > 0)
        ax.plot([args.floor, 1], [args.floor, 1], color=pf.INK_2, linewidth=1, linestyle=(0, (4, 3)), zorder=2)
        ax.scatter(replaced[shown & ~home], share[shown & ~home], s=30, color=pf.SERIES, edgecolor=pf.SURFACE,
                   linewidth=1, alpha=0.9, zorder=3)
        ax.scatter(replaced[home], share[home], s=60, color=c.LEAKED, edgecolor=pf.SURFACE, linewidth=1.5, zorder=4)
        # label every group, or the largest shares and the locations furthest above or below y = x, largest share first
        amplification = np.abs(np.log10(np.where(shown, share, 1) / np.where(shown, replaced, 1)))
        labelled = (range(len(units)) if args.groups
                    else set(np.argsort(-np.where(shown, share, 0))[:6]) | set(np.argsort(-amplification)[:4]))
        label(ax, [(replaced[j], share[j], units[j] if j in labelled else None)
                   for j in sorted(np.flatnonzero(shown), key=lambda j: -share[j])])
        ax.xaxis.set_major_formatter(percent)
        ax.yaxis.set_major_formatter(percent)
        ax.set_title(c.SHORT[k], color=pf.INK, fontsize=10, loc="left")
        ax.set_xlabel("Share of the replacing production (kg)", color=pf.INK_2, fontsize=8.5)
        hidden = (~shown & ~feed_only).sum()
        notes = ([f"{hidden} {unit_noun}{'s' if hidden > 1 else ''} below {args.floor:.2%} or negative"] if hidden
                 else [])
        if feed_only.any():
            notes.append(f"{feed_only.sum()} growing only feed, with {share[feed_only].sum():.1%} of the impact")
        if notes:
            ax.text(0.98, 0.02, "not shown: " + ",\nand ".join(notes), transform=ax.transAxes, ha="right",
                    va="bottom", fontsize=7, color=pf.INK_2)
    axes[0].set_ylabel("Share of the impact leaked", color=pf.INK_2, fontsize=8.5)
    c.save(fig, f"profile_{iso}_{c.stem(m)}{suffix}_shares.png", args.config, folder=c.PROFILES)
