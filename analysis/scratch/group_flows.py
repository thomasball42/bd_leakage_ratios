# -*- coding: utf-8 -*-
"""
Leakage between groups of countries, World Bank income groups or regions, at one
displacement size: `python group_flows.py [--groups income|region] [--panels lands net] [--units pct|abs ...]
[--metric M] [--item ITEM] [--size S] [--config CONFIG]` (default: income groups, both panels, lands as %
and net per kg, extinctions, the production portfolio, size 1).

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
    pct    as % of all A leaks (lands) or spares (net), as in _common.flow_panels
    abs    in the metric's unit per kg A displaces: G[A, B] over the production of
           A's countries (those with a result), the production-weighted mean of
           their per-kg rows; the absolute panels share a power of 10

Right of each matrix, the sum of each row, on the matrix's colour scale: all that
A leaks (lands) or its net change everywhere (net), per kg it displaces; as %, A's
leakage ratio - 1 (net only, as lands' rows all sum to 100%).
"""

import argparse
import textwrap
from pathlib import Path

import numpy as np

import _common as c

pf, fm, plt = c.pf, c.fm, c.plt

# the panels: their colour maps, and whether they diverge around 0
PANELS = {"lands": (c.BLUES, False), "net": (c.DIVERGING, True)}
DEFAULT_UNITS = {"lands": "pct", "net": "abs"}
# in abs --units, their colour bars' labels
ABSOLUTE_UNITS = {"lands": "{scale}{unit} leaked per kg A displaces", "net": "{scale}{unit}, net, per kg A displaces"}
SUPERSCRIPT = str.maketrans("-0123456789", "⁻⁰¹²³⁴⁵⁶⁷⁸⁹")

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--groups", choices=list(c.GROUPINGS), default="income", help="(default: income)")
parser.add_argument("--panels", nargs="+", choices=list(PANELS), default=list(PANELS),
                    help="the panels, in order (default: lands net)")
parser.add_argument("--units", nargs="+", choices=["pct", "abs"], default=None,
                    help="pct: as %% of all the displacing group leaks or spares; abs: in the metric's unit per kg it "
                         "displaces. One for all the panels, or one per panel, in --panels' order (default: lands "
                         "pct, net abs)")
parser.add_argument("--metric", choices=fm.METRICS, default="bd", help="(default: bd)")
parser.add_argument("--item", default=fm.PORTFOLIO, help="an item code or name, or portfolio (default)")
parser.add_argument("--size", type=float, default=1, help="the displacement size, the run's nearest (default: 1)")
parser.add_argument("--config", type=Path, default=c._run_paths.DEFAULT_CONFIG, help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()
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
    ax.tick_params(which="both", length=0, colors=pf.INK, labelsize=8)
    for (a, b), v in np.ndenumerate(values):
        if np.isfinite(v):
            ax.text(b, a, text(v), ha="center", va="center", fontsize=8 if rows <= 4 else 7,
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
values = {key: per_kg[key] if units[key] == "abs" else shares[key] for key in keys}

labels = [c.GROUP_LABELS.get(g, g) for g in groups]
n = len(groups)
totals = {key: units[key] == "abs" or key != "lands" for key in keys}  # lands' rows as % all sum to 100%
# per panel, its matrix's column, a gap and its total's if it has one, and a spacer before the next
ratios = [r for key in keys for r in ([n, 0.35, 1.3] if totals[key] else [n]) + [0.5]][:-1]
# one power of 10 for all the absolute panels' values, so that they compare
absolute = [key for key in keys if units[key] == "abs"]
e = exponent(*[values[key] for key in absolute], *[values[key].sum(axis=1) for key in absolute]) if absolute else 0

# the title and subtitle, wrapped to the figure's width, above the panels
size = pf.size_text(m, m["sizes"][i])
width = sum(4.6 + 1.1 * totals[key] for key in keys) + 1.5
heading = textwrap.wrap(f"{c.item_label(m)}: {pf.NAMES[args.metric]} between World Bank {args.groups} groups, "
                      f"displacing {size}", int((width - 0.3) * 72 / (12 * 0.58)))
subheading = textwrap.wrap("Every country displacing the same share of its production, in its own scenario, and summed "
                         "within each group" + (", per kg: over the group's production" if absolute else "")
                         + ". Leakage includes what each country replaces itself."
                         + (" Total: each row's sum." if any(totals.values()) else ""),
                         int((width - 0.3) * 72 / (8.5 * 0.58)))
header = 0.12 + 0.24 * len(heading) + 0.06 + 0.17 * len(subheading) + 0.15  # inches
height = 0.62 * n + 2.1 + header
fig = plt.figure(figsize=(width, height), facecolor=pf.SURFACE, layout="constrained")
fig.get_layout_engine().set(rect=(0, 0, 1, 1 - header / height), w_pad=0.02, wspace=0.01)
fig.text(0.1 / width, 1 - 0.12 / height, "\n".join(heading), ha="left", va="top", color=pf.INK, fontsize=12,
         linespacing=1.25)
fig.text(0.1 / width, 1 - (0.12 + 0.24 * len(heading) + 0.06) / height, "\n".join(subheading), ha="left", va="top",
         color=pf.INK_2, fontsize=8.5, linespacing=1.35)
grid = fig.add_gridspec(1, len(ratios), width_ratios=ratios)
col = 0
for p, key in enumerate(keys):
    title, unit = c.FLOW_PANELS[key]
    cmap, diverging = PANELS[key]
    matrix = values[key]
    total = matrix.sum(axis=1, keepdims=True)  # NaN for the rows that are NaN
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
    ax.set_xticks(range(n), labels, rotation=30 if n > 4 else 0, ha="right" if n > 4 else "center")
    ax.set_yticks(range(n), labels if p == 0 else [])
    ax.set_title(title, color=pf.INK, fontsize=10, loc="left")
    ax.set_xlabel("Where the impact lands (B)", color=pf.INK_2, fontsize=8.5)
    if p == 0:
        ax.set_ylabel("Displacing group (A)", color=pf.INK_2, fontsize=8.5)
    parents = [ax]
    if totals[key]:
        side = fig.add_subplot(grid[0, col + 2])
        heatmap(side, total, cmap, diverging, limit, text)
        side.set_xticks([])
        side.set_title("Total", color=pf.INK, fontsize=9)
        side.set_yticks(range(n), [])
        parents.append(side)
    col += (3 if totals[key] else 1) + 1

    bar = fig.colorbar(image, ax=parents, fraction=0.05, pad=0.02, aspect=25)
    bar.outline.set_visible(False)
    bar.ax.tick_params(length=0, labelsize=7, colors=pf.INK_2)
    if units[key] == "abs":
        bar.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v / 10.0 ** e:.3g}"))
    else:
        bar.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v * 100:.3g}%"))
    bar.set_label(unit, color=pf.INK_2, fontsize=8)

default = keys == list(PANELS) and units == DEFAULT_UNITS
suffix = "" if default else "".join(f"_{key}_{units[key]}" for key in keys)
c.save(fig, f"group_flows_{args.groups}_{c.stem(m)}_{args.metric}_size{m['sizes'][i]:.3g}{suffix}.png", args.config,
       folder=c.FLOWS)
