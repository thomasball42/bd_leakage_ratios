# -*- coding: utf-8 -*-
"""
Illustrative, not an analysis: a country's trade in every crop and animal product, on
a map, with feed apart: `python country_trade_map.py [ISO ...] [--zoom W E S N]
[--coverage F] [--feed-coverage F] [--labels N] [--no-subtitle] [--config CONFIG]`
(default: GBR, the world, every flow of at least a tonne, the feed flows between
other countries that carry 90% of their total, the 12 largest partners labelled). One
figure per country C, in the scratch figures' profiles/ folder:

  trade_map_{ISO}.png, or trade_map_{ISO}_zoom.png with --zoom

--zoom shows the part of the map between those longitudes and latitudes (or a little
more, to keep the map's shape), e.g. --zoom -13 32 35 62 for Europe, where a European
country's larger flows are too close together to tell apart on the world's: only the
flows between the countries in it, the arcs and dots drawn larger. The partners that
are labelled are the largest whose labels fit clear of the others' and the dots.

It's FLMP's trade, the run's, not its results: what's traded, not what leaks. Every
item is summed by weight, in FLMP's tonnes of primary equivalents (milk powder as
milk, sugar as cane or beet), and each flow runs from where the crop was grown or the
animal raised to where it's used, whatever it passes through on the way:
  imports   what C uses that was grown or raised abroad, but for its livestock's feed
  exports   what C grows or raises that's used abroad, but for livestock's feed there
  feed      the crops C's livestock eat that were grown abroad, and those C grows that
            livestock abroad eat (feed_mix_long's feed per kg x FLMP's production)
  feed behind imports
            the crops grown in one country and eaten by livestock in another, abroad,
            for the animal products C imports from the second (feed_mix_long's feed
            per kg x C's imports from it). Feed grown where the animals are isn't
            traded, and feed grown in C is in feed, so neither is here.
The first three are solid arcs, feed the straighter ones, and the last dashed. Each
grows wider and more opaque towards where it goes, and bows to the left of its
direction, so imports and exports part. The widths at the ends are on one
square-root scale, so the small flows show. The dots, at the capitals, are the
partners, their area all C trades with them, feed included. What C uses that it
produces itself isn't shown: its share is in the subtitle.

Feed is FLMP's, without the run's cap on feed rates (feed.max_ratio_to_median). FLMP
feeds some crops to livestock in larger amounts than C has of them, so the rest of
C's use of those is set to 0. Flows under a tonne are left out.

The map and the capitals are _map.py's. With --no-subtitle (or --nosub), the grey
description under the title is left out.
"""

import argparse
import sys
from pathlib import Path

import numpy as np

import _common as c
import _map

sys.path.insert(0, str(c._run_paths.REPO_DIR))
import _leakage_funcs  # noqa: E402

pf, plt = c.pf, c.plt
IMPORTS, EXPORTS, FEED = "#2a78d6", "#1baf7a", "#eb6834"  # the reference palette's slots 1, 3 and 2
WIDEST = 5.0  # the widest arc's width at its end, in points
MIN_TONNES = 1
DOT, SMALLEST_DOT = 150, 3  # the dots' areas, in points²
RING = 42  # the area of the ring at C's capital, in points²
ZOOMED = 2.2  # with --zoom, the arcs' widths and the dots' and ring's diameters are multiplied by this
ALPHA = 0.12, 0.75
TRADE_BOW, FEED_BOW = 0.3, 0.1
KEY = [0.1, 0.5, 2]  # the legend's flows, in Mt

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("countries", nargs="*", default=["GBR"], help="ISO3 codes (default: GBR)")
parser.add_argument("--zoom", nargs=4, type=float, metavar=("W", "E", "S", "N"),
                    help="the longitudes and latitudes of the part of the map shown, in degrees, e.g. -13 32 35 62 for "
                         "Europe (default: the world)")
parser.add_argument("--coverage", type=float, default=1,
                    help="the share of each of the imports, exports and feed that the flows drawn carry, the largest "
                         "first (default: 1, every flow of at least a tonne)")
parser.add_argument("--feed-coverage", type=float, default=0.9,
                    help="the same, of the feed between other countries, or 0 to leave it out (default: 0.9)")
parser.add_argument("--labels", type=int, default=12,
                    help="the partners labelled, those trading the most whose labels fit (default: 12)")
parser.add_argument("--no-subtitle", "--nosub", action="store_true",
                    help="leave out the grey description under the title")
parser.add_argument("--config", type=Path, default=c._run_paths.DEFAULT_CONFIG, help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()
if not 0 < args.coverage <= 1 or not 0 <= args.feed_coverage <= 1:
    parser.error("--coverage is a share, above 0 and at most 1, and --feed-coverage from 0 to 1")
if args.zoom and not (args.zoom[0] < args.zoom[1] and args.zoom[2] < args.zoom[3]):
    parser.error("--zoom is W E S N, with W < E and S < N")
grown = ZOOMED if args.zoom else 1

config, _, _, data_dir = c._run_paths.run_paths(args.config)
resolve, year, paths = c._run_paths._run_config.resolve, config["run"]["year"], config["paths"]
impacts_dir = (resolve(paths["impacts_dir"]) if paths["impacts_dir"] else
               c._run_paths.run_paths(args.config)[1].parent / "inputs" / "impacts_matrices")
codes, R = _leakage_funcs.load_trade_matrices(resolve(paths["flmp_results_dir"]), year)
feed_mix = _leakage_funcs.load_feed_mix(impacts_dir / f"feed_mix_long_{year}.csv", codes)
iso_of = _leakage_funcs.load_country_isos(data_dir)
isos = [iso_of.get(int(code), str(code)) for code in codes]
names = c.attributes()["name"].dropna().to_dict()
where, placed = _map.capitals(isos)
n = len(codes)


def name(iso: str) -> str:
    """The World Bank's name for a country, or its code where it has none."""
    return names.get(iso, iso)


def flows_of(k: int) -> dict[str, np.ndarray]:
    """C's (codes position k's) imports, exports, feed in and feed out by partner, in tonnes, and the feed behind
    its imports, by where it's grown (rows) and where it's eaten (columns)."""
    feed_in, feed_out = {i: np.zeros(n) for i in R}, {i: np.zeros(n) for i in R}  # by feed crop
    behind = np.zeros((n, n))
    for animal, mix in feed_mix.items():
        if animal not in R:
            continue
        produced, imported = R[animal].sum(axis=0), R[animal][k].copy()
        imported[k] = 0
        eater, origin = mix["producer_idx"].to_numpy(), mix["origin_idx"].to_numpy()
        for crop, g in mix.groupby("feed_item"):
            if crop not in R:
                continue
            p, o = g["producer_idx"].to_numpy(), g["origin_idx"].to_numpy()
            tonnes = g["feed_kg_per_kg"].to_numpy() * produced[p]
            np.add.at(feed_in[crop], o[p == k], tonnes[p == k])
            np.add.at(feed_out[crop], p[o == k], tonnes[o == k])
        np.add.at(behind, (origin, eater), mix["feed_kg_per_kg"].to_numpy() * imported[eater])
    out = {"imports": sum((R[i][k] - feed_in[i]).clip(0) for i in R),
           "exports": sum((R[i][:, k] - feed_out[i]).clip(0) for i in R),
           "feed_in": sum(feed_in.values()), "feed_out": sum(feed_out.values())}
    out["home"] = np.array([out["imports"][k]])
    for key in ["imports", "exports", "feed_in", "feed_out"]:
        out[key][k] = 0
    np.fill_diagonal(behind, 0)  # grown where it's eaten
    behind[k] = behind[:, k] = 0
    out["behind"] = behind
    return out


def largest(flow: np.ndarray, coverage: float) -> np.ndarray:
    """The positions in (flat) `flow` of the largest flows, of MIN_TONNES or more and between countries placed, that
    carry `coverage` of it, the smallest first."""
    usable = placed if flow.ndim == 1 else np.outer(placed, placed).ravel()
    values = flow.ravel()
    at = np.argsort(-values)
    at = at[(values[at] >= MIN_TONNES) & usable[at]]
    if not len(at) or not coverage:
        return at[:0]
    return at[:min(len(at), np.searchsorted(np.cumsum(values[at]) / values.sum(), coverage - 1e-9) + 1)][::-1]


def mt(tonnes: float) -> str:
    """An amount in Mt, to a decimal place."""
    return f"{tonnes / 1e6:.1f} Mt"


for iso in args.countries:
    if iso not in isos:
        print(f"No {iso} in FLMP's trade")
        continue
    k = isos.index(iso)
    if not placed[k]:
        print(f"{iso} has no capital; skipping")
        continue
    f = flows_of(k)
    own = f["home"][0] / (f["home"][0] + f["imports"].sum())  # of all C's use, whatever's in view
    if args.zoom:  # only what's between the countries in view
        left, right, low, high = _map.view(args.zoom)
        with np.errstate(invalid="ignore"):
            seen = (left < where[:, 0]) & (where[:, 0] < right) & (low < where[:, 1]) & (where[:, 1] < high)
        if not seen[k]:
            print(f"{iso}'s capital isn't in --zoom; skipping")
            continue
        for key in ["imports", "exports", "feed_in", "feed_out"]:
            f[key][~seen] = 0
        f["behind"][~seen] = 0
        f["behind"][:, ~seen] = 0
    traded = f["imports"] + f["exports"] + f["feed_in"] + f["feed_out"]  # with each partner
    if not traded.sum() > 0:
        print(f"{iso} trades nothing; skipping")
        continue
    drawn = {key: largest(f[key], args.coverage) for key in ["imports", "exports", "feed_in", "feed_out"]}
    behind = largest(f["behind"], args.feed_coverage)
    widest = max(f[key].max() for key in drawn)

    def width(tonnes: float) -> float:
        """An arc's width at its end, in points."""
        return grown * WIDEST * np.sqrt(tonnes / widest)

    for key in drawn:
        print(f"{iso} {key}: {mt(f[key].sum())}, {len(drawn[key])} flows drawn")
    print(f"{iso} feed behind imports: {mt(f['behind'].sum())}, {len(behind)} flows drawn, "
          f"carrying {f['behind'].ravel()[behind].sum() / max(f['behind'].sum(), 1e-9):.0%}")
    fig, ax, globe, legend = _map.frame(
        f"{name(iso)}: " + (f"trade in crops and animal products with the countries shown, {year}" if args.zoom else
                            f"all trade in crops and animal products, {year}"),
        "" if args.no_subtitle else
        f"Illustrative. Every product, summed by weight in tonnes of primary equivalents, from where it was grown or "
        f"raised to where it's used; the arcs grow towards where they go. Feed is drawn apart: what the country's own "
        f"livestock eat from abroad, what it grows for livestock abroad, and, dashed, the feed traded between other "
        f"countries for the animal products it imports (the largest flows, {args.feed_coverage:.0%} of it). Each dot's "
        f"area is all that's traded with that country. "
        + ("Only what's traded between the countries shown is drawn, and in the totals. " if args.zoom else "")
        + f"{own:.0%} of what's used at home, but for feed, is produced "
        f"there, and isn't shown.", args.zoom)

    # the arcs, the dashed ones under the rest, then the dots, the largest first, and C's ring
    here = where[k]
    _map.draw_arcs(ax, globe, [(where[o], where[p], width(f["behind"][o, p]), FEED)
                               for o, p in (divmod(cell, n) for cell in behind)],
                   alpha=(0.25, 0.8), taper=0.6, dash=0.012 / grown)
    for key, colour, bow in [("imports", IMPORTS, TRADE_BOW), ("exports", EXPORTS, TRADE_BOW),
                             ("feed_in", FEED, FEED_BOW), ("feed_out", FEED, FEED_BOW)]:
        _map.draw_arcs(ax, globe, [(where[j], here, width(f[key][j]), colour) if key in ("imports", "feed_in") else
                                   (here, where[j], width(f[key][j]), colour) for j in drawn[key]],
                       alpha=ALPHA, taper=0.35, bow=bow)
    partners = [j for j in np.argsort(-traded) if traded[j] >= MIN_TONNES and placed[j]]
    area = grown ** 2 * np.maximum(SMALLEST_DOT, DOT * traded / traded.max())
    ax.scatter(*where[partners].T, s=area[partners], facecolor=_map.DOTS, alpha=0.85, edgecolor=pf.SURFACE,
               linewidth=0.6, zorder=2)
    ax.scatter(*here, s=grown ** 2 * RING, facecolor=pf.SURFACE, edgecolor=pf.INK, linewidth=1.8, zorder=3)

    # C and its largest partners whose labels fit, named
    radii = np.sqrt(area) / 2
    ring = grown * np.sqrt(RING) / 2 + 0.9
    _map.label(ax, [(here, ring, name(iso), "bold")]
               + [(where[j], radii[j], name(isos[j]), "normal") for j in partners[:4 * args.labels]],
               [(here, ring)] + [(where[j], radii[j]) for j in partners if radii[j] > 2], clear=args.labels)

    # the legend: what the arcs are, with their totals, their widths, and the dots
    entries = [(IMPORTS, "-", f"Imports, {mt(f['imports'].sum())}"),
               (EXPORTS, "-", f"Exports, {mt(f['exports'].sum())}"),
               (FEED, "-", f"Feed: {mt(f['feed_in'].sum())} for its livestock, {mt(f['feed_out'].sum())} for livestock "
                           "abroad"),
               (FEED, (0, (1.6, 1.2)), f"Feed between other countries, for the animal products it imports, "
                                       f"{mt(f['behind'].sum())}")]
    legend.text(0, 1.9, "What's traded, by weight", ha="left", va="top", fontsize=8.5, color=pf.INK_2)
    for p, (colour, style, text) in enumerate(entries):
        x, y = [0, 2.6][p // 2], 1.1 - 0.7 * (p % 2)
        legend.plot([x, x + 0.4], [y, y], color=colour, linewidth=3, linestyle=style,
                    solid_capstyle="round", dash_capstyle="butt")
        legend.text(x + 0.55, y, text, ha="left", va="center", fontsize=8.5, color=pf.INK)
    legend.text(9.3, 1.9, "Flow, at its end, Mt", ha="left", va="top", fontsize=8.5, color=pf.INK_2)
    for p, share in enumerate(KEY):
        x = 9.3 + 1.1 * p
        legend.plot([x, x + 0.4], [0.75, 0.75], color=pf.INK_2, linewidth=width(share * 1e6), solid_capstyle="butt")
        legend.text(x + 0.5, 0.75, f"{share:g}", ha="left", va="center", fontsize=8.5, color=pf.INK)
    legend.text(12.6, 1.9, "All traded with a country, Mt", ha="left", va="top", fontsize=8.5, color=pf.INK_2)
    for p, share in enumerate([0.1, 1, 3]):
        x = 12.75 + 0.9 * p
        legend.scatter([x], [0.75], s=grown ** 2 * max(SMALLEST_DOT, DOT * share * 1e6 / traded.max()),
                       facecolor=_map.DOTS, alpha=0.75, edgecolor=pf.SURFACE, linewidth=0.6, clip_on=False)
        legend.text(x + 0.28, 0.75, f"{share:g}", ha="left", va="center", fontsize=8.5, color=pf.INK)

    c.save(fig, f"trade_map_{iso}{'_zoom' if args.zoom else ''}.png", args.config, folder=c.PROFILES)
