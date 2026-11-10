# -*- coding: utf-8 -*-
"""
Every country's leakage abroad at once, on a map, at one displacement size:
`python flow_map.py [--coverage F] [--labels N] [--metric M] [--item ITEM] [--size S] [--config CONFIG]`
(default: the flows that carry 95% of the leakage across borders, the 10 countries
receiving the most labelled, extinctions, the production portfolio, size
1).

Each country's row is weighted by its production, as if every country displaced
the same share of its production, each in its own scenario, as in group_flows.py.
Each flow is an arc from the displacing country's capital to that of the country
where the leakage lands, bowing to the left of its direction (by at most BOW), so
that the flows each way between two countries part, and growing wider and more
opaque towards where it lands. Its colour is the displacing country's region's (as
in flow_chord.py), and its width, at the end, the flow, on a square-root scale, so
that the small flows show. The largest that carry --coverage of the leakage across
borders are drawn, the smallest first, within the globe's edge. Each country is a dot at its capital, its
area what it receives from abroad. The leakage that stays in the displacing country
(what it replaces itself) isn't shown: its share is in the subtitle.

The map is the Equal Earth projection, of Natural Earth's 1:110m land (public
domain, downloaded once into data/naturalearth/), from 58°S. The capitals are the
World Bank's (data/worldbank/countries.json, from country_attributes.py), and
CAPITALS' for the countries it hasn't.
"""

import argparse
import json
import textwrap
from pathlib import Path

import numpy as np
import requests
from matplotlib import patheffects
from matplotlib.collections import LineCollection, PolyCollection

import _common as c

pf, fm, plt = c.pf, c.fm, c.plt
LAND_URL = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_land.geojson"
LAND_PATH = c.DATA_DIR / "naturalearth" / "ne_110m_land.geojson"
LAND = "#e9e8e4"  # a step below the surface, recessive
# the capitals, longitude and latitude, of the countries with none in the World Bank's list: 299 is FAO's Palestine
CAPITALS = {"TWN": (121.56, 25.04), "COK": (-159.78, -21.21), "NIU": (-169.92, -19.06), "TKL": (-171.85, -9.20),
            "299": (35.20, 31.90)}
SOUTH = -58  # the map's southern edge, in degrees
WIDEST = 7.0  # the widest arc's width at its end, in points
POINTS = 60  # the points along each arc
BOW = 0.3, 0.35  # how far each arc's control point is from the straight line, as a share of its length, and at
# most, on the unit sphere's scale, so that the longest arcs stay on the map
DOTS = "#52514e"  # the dots' fill: secondary ink, under DOT_ALPHA
DOT_ALPHA = 0.6
DOT = 700  # the largest dot's area, in points²

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--coverage", type=float, default=0.95,
                    help="the share of the cross-border leakage that the flows drawn carry, the largest first, or 1 "
                         "for every flow (default: 0.95)")
parser.add_argument("--labels", type=int, default=10, help="the countries receiving the most labelled (default: 10)")
parser.add_argument("--metric", choices=fm.METRICS, default="bd", help="(default: bd)")
parser.add_argument("--item", default=fm.PORTFOLIO, help="an item code or name, or portfolio (default)")
parser.add_argument("--size", type=float, default=1, help="the displacement size, the run's nearest (default: 1)")
parser.add_argument("--config", type=Path, default=c._run_paths.DEFAULT_CONFIG, help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()
if not 0 < args.coverage <= 1:
    parser.error("--coverage is a share, above 0 and at most 1")


def equal_earth(lon: np.ndarray, lat: np.ndarray) -> np.ndarray:
    """Šavrič, Patterson & Jenny's (2018) Equal Earth projection of longitudes and
    latitudes, in degrees, onto a unit sphere's x and y."""
    a1, a2, a3, a4 = 1.340264, -0.081106, 0.000893, 0.003796
    theta = np.arcsin(np.sqrt(3) / 2 * np.sin(np.radians(lat)))
    t2 = theta ** 2
    x = 2 * np.sqrt(3) * np.radians(lon) * np.cos(theta) / (3 * (9 * a4 * t2 ** 4 + 7 * a3 * t2 ** 3 + 3 * a2 * t2 + a1))
    y = theta * (a1 + t2 * (a2 + t2 * (a3 + a4 * t2)))
    return np.column_stack([x, y])


def land() -> list[np.ndarray]:
    """Natural Earth's land, each polygon's outer ring, every 1° or closer so that it
    follows the projection's curves, projected and clipped to SOUTH."""
    if not LAND_PATH.exists():
        print(f"Downloading {LAND_URL}")
        r = requests.get(LAND_URL, timeout=60)
        r.raise_for_status()
        LAND_PATH.parent.mkdir(parents=True, exist_ok=True)
        LAND_PATH.write_bytes(r.content)
    rings = []
    for feature in json.loads(LAND_PATH.read_text(encoding="utf-8"))["features"]:
        geometry = feature["geometry"]
        polygons = geometry["coordinates"] if geometry["type"] == "MultiPolygon" else [geometry["coordinates"]]
        for polygon in polygons:
            ring = np.array(polygon[0], dtype=float)
            steps = np.maximum(1, np.ceil(np.abs(np.diff(ring, axis=0)).max(axis=1))).astype(int)
            dense = np.concatenate([np.linspace(p, q, s, endpoint=False) for p, q, s in zip(ring, ring[1:], steps)])
            dense[:, 1] = dense[:, 1].clip(SOUTH)
            rings.append(equal_earth(dense[:, 0], dense[:, 1]))
    return rings


m = c.load(args.item, args.config)
i = fm.size_index(m, args.size)
isos, flow, home = c.cross_border(m, args.metric, i)
n = len(isos)
region = c.region_of(isos)
colour = {**c.REGION_COLOURS, c.NO_REGION: c.MUTED}
total = flow.sum()
into = flow.sum(axis=0)

# each country's capital, projected
capitals = dict(CAPITALS)
for record in json.loads((c.DATA_DIR / "worldbank" / "countries.json").read_text(encoding="utf-8")):
    if record["longitude"] and record["latitude"]:
        capitals.setdefault(record["id"], (float(record["longitude"]), float(record["latitude"])))
placed = np.array([iso in capitals for iso in isos])
if not placed.all():
    lost = flow[~placed].sum() + flow[:, ~placed].sum() - flow[np.ix_(~placed, ~placed)].sum()
    print(f"No capital: {', '.join(np.array(isos)[~placed])}, left out, with {lost / total:.2%} of the leakage")
lonlat = np.array([capitals.get(iso, (np.nan, np.nan)) for iso in isos])
where = equal_earth(lonlat[:, 0], lonlat[:, 1])

# the flows drawn: the largest, until they carry --coverage of the leakage, of those between countries placed
cells = np.argsort(flow, axis=None)[::-1]
cells = cells[(flow.flat[cells] > 0) & placed[cells // n] & placed[cells % n]]
carried = np.cumsum(flow.flat[cells]) / total
drawn = cells[:min(len(cells), np.searchsorted(carried, args.coverage - 1e-9) + 1)]
print(f"{len(drawn):,} flows of {len(cells):,}, carrying {flow.flat[drawn].sum() / total:.2%} of the leakage "
      "across borders")
largest = flow.flat[drawn[0]]


def width(f: np.ndarray) -> np.ndarray:
    """An arc's width at its end, in points, for flow f."""
    return WIDEST * np.sqrt(f / largest)


# each arc, the smallest first, as segments along a quadratic Bézier curve that grow wider and more opaque
s = np.linspace(0, 1, POINTS)[:, None]
segments, widths, colours = [], [], []
for cell in drawn[::-1]:
    a, b = divmod(cell, n)
    p0, p1 = where[a], where[b]
    d = p1 - p0
    bow = min(BOW[0], BOW[1] / np.hypot(*d))
    control = (p0 + p1) / 2 + bow * np.array([-d[1], d[0]])  # to the left of the direction
    path = (1 - s) ** 2 * p0 + 2 * (1 - s) * s * control + s ** 2 * p1
    segments.extend(np.stack([path[:-1], path[1:]], axis=1))
    along = (s[:-1, 0] + s[1:, 0]) / 2
    widths.extend(np.maximum(0.25, width(flow.flat[cell]) * (0.15 + 0.85 * along ** 0.8)))
    rgba = np.tile(plt.matplotlib.colors.to_rgba(colour[region[isos[a]]]), (POINTS - 1, 1))
    rgba[:, 3] = 0.08 + 0.8 * along ** 1.5
    colours.extend(rgba)

# the title and subtitle, wrapped to the figure's width
fig_width = 15.0
size = pf.size_text(m, m["sizes"][i])
heading = textwrap.wrap(f"{c.item_label(m)}: {pf.NAMES[args.metric]} leaking abroad, {len(cells):,} flows "
                        f"between {n} countries, displacing {size}", int((fig_width - 0.6) * 72 / (14 * 0.58)))
subheading = textwrap.wrap(
    f"Every country displacing the same share of its production, each in its own scenario. Each arc runs from the "
    f"displacing country's capital, in its region's colour, to that of where the leakage lands, growing towards it; "
    f"its width there is the flow. The {len(drawn):,} largest flows, {flow.flat[drawn].sum() / total:.0%} of the "
    f"leakage across borders, are drawn. Each dot's area is what the country receives from abroad. {home:.0%} of "
    f"all the leakage stays in the displacing country, and isn't shown.", int((fig_width - 0.6) * 72 / (9 * 0.58)))
header = 0.25 + 0.3 * len(heading) + 0.08 + 0.19 * len(subheading) + 0.15  # inches
rings = land()
x0, x1 = -2.75, 2.75
y0, y1 = equal_earth(np.array([0]), np.array([SOUTH]))[0, 1] - 0.03, 1.33
map_height = (fig_width - 0.4) * (y1 - y0) / (x1 - x0)
legend_height = 1.1
fig_height = header + map_height + legend_height
fig = plt.figure(figsize=(fig_width, fig_height), facecolor=pf.SURFACE)
fig.text(0.3 / fig_width, 1 - 0.25 / fig_height, "\n".join(heading), ha="left", va="top", color=pf.INK, fontsize=14,
         linespacing=1.25)
fig.text(0.3 / fig_width, 1 - (0.25 + 0.3 * len(heading) + 0.08) / fig_height, "\n".join(subheading), ha="left",
         va="top", color=pf.INK_2, fontsize=9, linespacing=1.4)
ax = fig.add_axes((0.2 / fig_width, legend_height / fig_height, (fig_width - 0.4) / fig_width,
                   map_height / fig_height))
ax.set_aspect("equal")
ax.set_xlim(x0, x1)
ax.set_ylim(y0, y1)
ax.axis("off")

# the globe's edge, the land, the arcs and the dots, in that order
edge = np.concatenate([equal_earth(np.full(90, 180), np.linspace(SOUTH, 90, 90)),
                       equal_earth(np.full(90, -180), np.linspace(90, SOUTH, 90))])
globe = ax.add_patch(plt.Polygon(edge, facecolor="none", edgecolor=pf.GRID, linewidth=0.8, zorder=0))
ax.add_collection(PolyCollection(rings, facecolors=LAND, edgecolors="none", zorder=0))
# butt caps: round ones overlap, and show as beads along the wider, translucent arcs
arcs = ax.add_collection(LineCollection(segments, linewidths=widths, colors=colours, capstyle="butt", zorder=1))
arcs.set_clip_path(globe)
ax.scatter(*where[placed].T, s=np.maximum(3, DOT * into[placed] / into.max()), facecolor=DOTS, alpha=DOT_ALPHA,
           edgecolor=pf.SURFACE, linewidth=0.6, zorder=2)

# the countries receiving the most, named
names = c.attributes()["name"].dropna().to_dict()
halo = [patheffects.withStroke(linewidth=2.5, foreground=pf.SURFACE)]
for k in [k for k in np.argsort(-into) if placed[k]][:args.labels]:
    radius = np.sqrt(max(3, DOT * into[k] / into.max()) / np.pi)  # the dot's, in points
    ax.annotate(f"{names.get(isos[k], isos[k])} {into[k] / total:.1%}", where[k], xytext=(radius * 0.75 + 3, 3),
                textcoords="offset points", ha="left", va="bottom", fontsize=8.5, color=pf.INK, path_effects=halo,
                zorder=3)

# the legend: the displacing regions' colours, the arcs' widths and the dots' areas
legend = fig.add_axes((0.3 / fig_width, 0.1 / fig_height, (fig_width - 0.6) / fig_width,
                       (legend_height - 0.2) / fig_height))
legend.axis("off")
legend.set_xlim(0, 15)
legend.set_ylim(0, 2)
legend.text(0, 1.9, "From the displacing country's region", ha="left", va="top", fontsize=8.5, color=pf.INK_2)
regions = [r for r in [*c.REGION_COLOURS, c.NO_REGION] if r in region.values()]
for p, r in enumerate(regions):
    x, y = 2.05 * (p % 4), 1.1 - 0.7 * (p // 4)
    legend.plot([x, x + 0.35], [y, y], color=colour[r], linewidth=3, solid_capstyle="round")
    legend.text(x + 0.5, y, c.GROUP_LABELS.get(r, r), ha="left", va="center", fontsize=8.5, color=pf.INK)
legend.text(8.6, 1.9, "Flow, at its end, % of the leakage across borders", ha="left", va="top", fontsize=8.5,
            color=pf.INK_2)
for p, share in enumerate([0.1, 1, 5]):
    x = 8.6 + 1.1 * p
    legend.plot([x, x + 0.4], [0.75, 0.75], color=pf.INK_2, linewidth=width(np.array(share / 100 * total)),
                solid_capstyle="butt")
    legend.text(x + 0.5, 0.75, f"{share:g}%", ha="left", va="center", fontsize=8.5, color=pf.INK)
legend.text(12.3, 1.9, "Received from abroad, %", ha="left", va="top", fontsize=8.5, color=pf.INK_2)
for p, share in enumerate([1, 5, 15]):
    x = 12.45 + 0.95 * p
    legend.scatter([x], [0.75], s=DOT * share / 100 * total / into.max(), facecolor=DOTS, alpha=DOT_ALPHA,
                   edgecolor=pf.SURFACE, linewidth=0.6, clip_on=False)
    legend.text(x + 0.3, 0.75, f"{share:g}%", ha="left", va="center", fontsize=8.5, color=pf.INK)

c.save(fig, f"flow_map_{c.stem(m)}_{args.metric}_size{m['sizes'][i]:.3g}.png", args.config, folder=c.FLOWS)
