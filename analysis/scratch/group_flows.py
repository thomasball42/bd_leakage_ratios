# -*- coding: utf-8 -*-
"""
Leakage between groups of countries, World Bank income groups or regions, at one
displacement size: `python group_flows.py [--groups income|region] [--panels lands net] [--units pct|abs ...]
[--metric M] [--item ITEM] [--size S] [--no-subtitle] [--font-scale X] [--config CONFIG]` (default: income
groups, both panels as %, extinctions, the production portfolio, size 1).

Each country's row is weighted by its production, as if every country displaced
the same share of its production, each in its own scenario, and the rows and
columns summed within each group: G[A, B] is what A's countries' displacements
leak to (or spare in) B's. Countries with no group (no World Bank income group,
such as Venezuela's, or not in the World Bank's list, such as Taiwan) are left
out, and the share of the impact that leaves out is printed. The panels, either
or both, in the order given:
    lands  where A's leakage lands
    net    the net change in B, leaked - spared
each in either of --units:
    pct    as % of all A leaks (lands), as in _common.flow_panels, or of all A
           spares everywhere (net), the locations with no group too, as
           country_profile.py's net global effect is of all a country spares
    abs    in the metric's unit per kg A displaces: G[A, B] over the production of
           A's countries (those with a result), the production-weighted mean of
           their per-kg rows; the absolute panels share a power of 10

Right of each matrix, a total, on the matrix's colour scale. In abs units, the sum
of each row: all that A leaks (lands) or its net change everywhere (net), per kg
it displaces. With net as %, A's overall leakage ratio: all A's countries leak ÷ all they
spare, everywhere, as country_profile.py gives a country's, so a group of one
country would have that country's. Its colour is the ratio - 1's, which is the net
row's sum with the little that lands in the locations with no group. Lands as %
has none, as its rows all sum to 100%.

With --no-subtitle (or --nosub), the grey description under the title is left
out. --font-scale multiplies every font size, e.g. 2 doubles them; the cells keep
their size and the figure grows around them to hold the text, but for cells too
narrow for their numbers, which widen until they fit, and the groups' labels under
the matrices turn where they'd overlap.
"""

import argparse
import textwrap
from pathlib import Path

import numpy as np

import _common as c

pf, fm, plt = c.pf, c.fm, c.plt

# the panels: their colour maps, and whether they diverge around 0
PANELS = {"lands": (c.BLUES, False), "net": (c.DIVERGING, True)}
DEFAULT_UNITS = {"lands": "pct", "net": "pct"}
# in abs --units, their colour bars' labels
ABSOLUTE_UNITS = {"lands": "{scale}{unit} leaked per kg A displaces", "net": "{scale}{unit}, net, per kg A displaces"}
SUPERSCRIPT = str.maketrans("-0123456789", "⁻⁰¹²³⁴⁵⁶⁷⁸⁹")

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--groups", choices=list(c.GROUPINGS), default="income", help="(default: income)")
parser.add_argument("--panels", nargs="+", choices=list(PANELS), default=list(PANELS),
                    help="the panels, in order (default: lands net)")
parser.add_argument("--units", nargs="+", choices=["pct", "abs"], default=None,
                    help="pct: as %% of all the displacing group leaks or spares; abs: in the metric's unit per kg it "
                         "displaces. One for all the panels, or one per panel, in --panels' order (default: pct)")
parser.add_argument("--metric", choices=fm.METRICS, default="bd", help="(default: bd)")
parser.add_argument("--item", default=fm.PORTFOLIO, help="an item code or name, or portfolio (default)")
parser.add_argument("--size", type=float, default=1, help="the displacement size, the run's nearest (default: 1)")
parser.add_argument("--no-subtitle", "--nosub", action="store_true",
                    help="leave out the grey description under the title")
parser.add_argument("--font-scale", "--fontsize-scale", type=float, default=1, metavar="X", help=c.FONT_SCALE_HELP)
parser.add_argument("--config", type=Path, default=c._run_paths.DEFAULT_CONFIG, help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()
if not args.font_scale > 0:
    parser.error(f"--font-scale must be above 0, not {args.font_scale}")
c.set_font_scale(args.font_scale)
keys = list(dict.fromkeys(args.panels))
if args.units is None:
    units = {key: DEFAULT_UNITS[key] for key in keys}
elif len(args.units) in (1, len(keys)):
    units = dict(zip(keys, args.units * len(keys) if len(args.units) == 1 else args.units))
else:
    parser.error(f"--units takes one unit, or one per panel ({len(keys)}), not {len(args.units)}")


def number(x: float, signed: bool) -> str:
    """x to 2 significant figures, with no exponent (all of 1000 and more's digits)."""
    decimals = max(0, 1 - int(np.floor(np.log10(abs(x))))) if x else 0
    return f"{x:{'+' if signed else ''}.{decimals}f}"


def exponent(*arrays: np.ndarray) -> int:
    """The power of 10 that the arrays' values are shown in units of, so that the
    largest is from 1 to 10."""
    top = max(np.nanmax(np.abs(a)) for a in arrays)
    return int(np.floor(np.log10(top))) if top > 0 else 0


def heatmap(ax: plt.Axes, values: np.ndarray, cmap, diverging: bool, limit: float, text) -> plt.cm.ScalarMappable:
    """values as cells, each labelled text(v), on a colour scale from 0 (or from
    -limit, where diverging) to limit, with the 2px surface gap between them."""
    ax.set_facecolor(pf.SURFACE)
    for spine in ax.spines.values():
        spine.set_visible(False)
    image = ax.imshow(values, cmap=cmap, vmin=-limit if diverging else 0, vmax=limit, aspect="auto")
    rows, cols = values.shape
    ax.set_xticks(np.arange(cols + 1) - 0.5, minor=True)
    ax.set_yticks(np.arange(rows + 1) - 0.5, minor=True)
    ax.grid(which="minor", color=pf.SURFACE, linewidth=2)
    ax.tick_params(which="both", length=0, colors=pf.INK, labelsize=c.fs(8))
    for (a, b), v in np.ndenumerate(values):
        if np.isfinite(v):
            ax.text(b, a, text(v), ha="center", va="center", fontsize=c.fs(8 if rows <= 4 else 7),
                    color="#ffffff" if abs(v) / limit > 0.55 else pf.INK)
    return image


m = c.load(args.item, args.config)
i = fm.size_index(m, args.size)
column, groups = c.GROUPINGS[args.groups]
unit_of = c.attributes()[column].dropna().to_dict()
leaked, spared = c.flows(m, args.metric, i, unit_of, groups)
kg = c.production(m, args.metric, i, unit_of, groups)[:, None]
with np.errstate(divide="ignore", invalid="ignore"):
    per_kg = {"lands": np.where(kg > 0, leaked / kg, np.nan), "net": np.where(kg > 0, (leaked - spared) / kg, np.nan)}
shares = c.flow_panels(leaked, spared)
# all A's countries leak and spare everywhere, as the profiles sum a country's: A's leakage ratio, and net as % of all
# A spares there, so that the row and what lands in the locations with no group sum to the ratio - 1
leaked_all, spared_all = c.everywhere(m, args.metric, i, unit_of, groups)
with np.errstate(divide="ignore", invalid="ignore"):
    ratio = np.where(spared_all > 0, leaked_all / spared_all, np.nan)
    shares["net"] = np.where(spared_all[:, None] > 0, (leaked - spared) / spared_all[:, None], np.nan)
print("Leakage ratios, all leaked ÷ all spared: " + ", ".join(f"{g} {r:.3f}" for g, r in zip(groups, ratio)))
values = {key: per_kg[key] if units[key] == "abs" else shares[key] for key in keys}

labels = [c.GROUP_LABELS.get(g, g) for g in groups]
n = len(groups)
totals = {key: units[key] == "abs" or key != "lands" for key in keys}  # lands' rows as % all sum to 100%
as_ratio = {key: key == "net" and units[key] == "pct" for key in keys}  # the totals that are the leakage ratio
# per panel, its matrix's column, a gap and its total's if it has one, and a spacer before the next
ratios = [r for key in keys for r in ([n, 0.35, 1.3] if totals[key] else [n]) + [0.5]][:-1]
# one power of 10 for all the absolute panels' values, so that they compare
absolute = [key for key in keys if units[key] == "abs"]
e = exponent(*[values[key] for key in absolute], *[values[key].sum(axis=1) for key in absolute]) if absolute else 0

# the title and subtitle, wrapped to the figure's width, above the panels. The figure is the cells' inches and, around
# them, the text's, which grow with the --font-scale: per panel, 3.9 inches of cells and 0.7 of the colour bar's text,
# and 1.1 for a total's column; 1.5 for the groups' labels; and 0.62 a row of cells and 2.1 for the titles and labels
scale = c.FONT_SCALE
size = pf.size_text(m, m["sizes"][i])
width = sum(3.9 + 0.7 * scale + 1.1 * totals[key] for key in keys) + 1.5 * scale
heading = textwrap.wrap(f"{c.item_label(m)}: {pf.NAMES[args.metric]} between World Bank {args.groups} groups, "
                        f"displacing {size}", int((width - 0.3) * 72 / (c.fs(12) * 0.58)))
description = ("Every country displacing the same share of its production, in its own scenario, and summed within "
               "each group" + (", per kg: over the group's production" if absolute else "")
               + ". Leakage includes what each country replaces itself."
               + (" Total: each row's sum." if any(totals[k] and not as_ratio[k] for k in keys) else "")
               + (" Overall leakage ratio: all the group's countries leak ÷ all they spare, everywhere, as in the "
                  "countries' profiles; coloured as the ratio - 1, the net row's sum." if any(as_ratio.values())
                  else ""))
subheading = [] if args.no_subtitle else textwrap.wrap(description, int((width - 0.3) * 72 / (c.fs(8.5) * 0.58)))
# inches: the title's lines, and the subtitle's under them, if there is one
title_top, subtitle_top = 0.12 * scale, (0.12 + 0.24 * len(heading) + 0.06) * scale
header = (0.12 + 0.24 * len(heading) + (0.06 + 0.17 * len(subheading) if subheading else 0) + 0.15) * scale
height = 0.62 * n + 2.1 * scale + header
fig = plt.figure(figsize=(width, height), facecolor=pf.SURFACE, layout="constrained")
fig.get_layout_engine().set(rect=(0, 0, 1, 1 - header / height), w_pad=0.02, wspace=0.01)
header_texts = [fig.text(0.1 / width, 1 - title_top / height, "\n".join(heading), ha="left", va="top",
                         color=pf.INK, fontsize=c.fs(12), linespacing=1.25)]
if subheading:
    header_texts.append(fig.text(0.1 / width, 1 - subtitle_top / height, "\n".join(subheading), ha="left",
                                 va="top", color=pf.INK_2, fontsize=c.fs(8.5), linespacing=1.35))
grids, matrices = [], []  # every heatmap's axes and its columns, and the matrices' axes
grid = fig.add_gridspec(1, len(ratios), width_ratios=ratios)
col = 0
for p, key in enumerate(keys):
    title, unit = c.FLOW_PANELS[key]
    cmap, diverging = PANELS[key]
    matrix = values[key]
    # NaN for the rows that are NaN; the leakage ratio as its ratio - 1, on the net's scale
    total = (ratio - 1)[:, None] if as_ratio[key] else matrix.sum(axis=1, keepdims=True)
    both = np.concatenate([matrix, total], axis=1) if totals[key] else matrix
    finite = both[np.isfinite(both)]
    limit = ((np.abs(finite).max() if diverging else finite.max()) if finite.size else 0) or 1
    if units[key] == "abs":
        unit = ABSOLUTE_UNITS[key].format(scale=f"10{str(e).translate(SUPERSCRIPT)} " if e else "",
                                          unit=pf.UNITS[args.metric])

        def text(v, diverging=diverging):
            return number(v / 10.0 ** e, diverging)
    else:
        unit = unit.format(unit="group")

        def text(v, diverging=diverging):
            return f"{v * 100:{'+' if diverging else ''}.1f}%"

    ax = fig.add_subplot(grid[0, col])
    image = heatmap(ax, matrix, cmap, diverging, limit, text)
    grids.append((ax, n))
    matrices.append(ax)
    ax.set_xticks(range(n), labels, rotation=30 if n > 4 else 0, ha="right" if n > 4 else "center")
    ax.set_yticks(range(n), labels if p == 0 else [])
    ax.set_title(title, color=pf.INK, fontsize=c.fs(10), loc="left")
    ax.set_xlabel("Where the impact lands (B)", color=pf.INK_2, fontsize=c.fs(8.5))
    if p == 0:
        ax.set_ylabel("Displacing group (A)", color=pf.INK_2, fontsize=c.fs(8.5))
    parents = [ax]
    if totals[key]:
        side = fig.add_subplot(grid[0, col + 2])
        heatmap(side, total, cmap, diverging, limit, (lambda v: f"{v + 1:.2f}") if as_ratio[key] else text)
        side.set_xticks([])
        # wrapped to the column's width, about 13 characters at the --font-scale of 1
        side.set_title(textwrap.fill("Overall leakage ratio", max(7, round(13 / scale))) if as_ratio[key] else "Total",
                       color=pf.INK, fontsize=c.fs(9))
        side.set_yticks(range(n), [])
        parents.append(side)
        grids.append((side, 1))
    col += (3 if totals[key] else 1) + 1

    bar = fig.colorbar(image, ax=parents, fraction=0.05, pad=0.02, aspect=25)
    bar.outline.set_visible(False)
    bar.ax.tick_params(length=0, labelsize=c.fs(7), colors=pf.INK_2)
    if units[key] == "abs":
        bar.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v / 10.0 ** e:.3g}"))
    else:
        bar.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v * 100:.3g}%"))
    bar.set_label(unit, color=pf.INK_2, fontsize=c.fs(8))

# laid out, the text the cells hold fitted to them (at a large --font-scale, or with many groups): the cells widened,
# and the figure with them, until every cell's number fits its cell with a little room, and the groups' labels under
# the matrices turned where they'd overlap side by side
renderer = fig.canvas.get_renderer()
for _ in range(4):
    fig.draw_without_rendering()
    spans = [ax.get_window_extent(renderer).width for ax, _ in grids]  # in pixels
    over = max(t.get_window_extent(renderer).width / (span / cols)
               for (ax, cols), span in zip(grids, spans) for t in ax.texts) / 0.9
    if over <= 1:
        break
    fig.set_figwidth(fig.get_figwidth() + sum(spans) / fig.dpi * (over - 1))
    for text in header_texts:
        text.set_x(0.1 / fig.get_figwidth())
if n <= 4:  # more are turned already
    for ax in matrices:
        boxes = [t.get_window_extent(renderer) for t in ax.get_xticklabels()]
        if any(a.x1 + 0.05 * fig.dpi > b.x0 for a, b in zip(boxes, boxes[1:])):
            plt.setp(ax.get_xticklabels(), rotation=30, ha="right")

default = keys == list(PANELS) and units == DEFAULT_UNITS
suffix = "" if default else "".join(f"_{key}_{units[key]}" for key in keys)
c.save(fig, f"group_flows_{args.groups}_{c.stem(m)}_{args.metric}_size{m['sizes'][i]:.3g}{suffix}.png", args.config,
       folder=c.FLOWS)
