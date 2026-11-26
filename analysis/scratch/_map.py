# -*- coding: utf-8 -*-
"""
What the flow maps share (flow_map.py, country_flow_map.py): the Equal Earth
projection, of Natural Earth's 1:110m land (public domain, downloaded once into
data/naturalearth/), from 58°S, the countries' capitals, the World Bank's
(data/worldbank/countries.json, from country_attributes.py) and CAPITALS' for the
countries it hasn't, the arcs between them, and the figure they're drawn in.
"""

import json
import textwrap

import numpy as np
import requests
from matplotlib import patheffects
from matplotlib.collections import LineCollection, PolyCollection
from matplotlib.text import Annotation
from matplotlib.transforms import Bbox

import _common as c

pf, plt = c.pf, c.plt
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
SMALLEST_DOT = 3  # and the smallest's
FIG_WIDTH = 15.0  # inches
LEGEND_HEIGHT = 1.1
# where a label can go around its dot, in order of preference, as the direction from it and its alignment
PLACES = [(0.7, 0.7, "left", "bottom"), (1, 0, "left", "center"), (0.7, -0.7, "left", "top"),
          (-0.7, 0.7, "right", "bottom"), (-1, 0, "right", "center"), (-0.7, -0.7, "right", "top"),
          (0, 1, "center", "bottom"), (0, -1, "center", "top")]


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


def capitals(isos: list[str]) -> tuple[np.ndarray, np.ndarray]:
    """Where each ISO3's capital is on the map, projected (one row each), and
    whether it has one, (where, placed): NaN and False for those with none."""
    known = dict(CAPITALS)
    for record in json.loads((c.DATA_DIR / "worldbank" / "countries.json").read_text(encoding="utf-8")):
        if record["longitude"] and record["latitude"]:
            known.setdefault(record["id"], (float(record["longitude"]), float(record["latitude"])))
    lonlat = np.array([known.get(iso, (np.nan, np.nan)) for iso in isos])
    return equal_earth(lonlat[:, 0], lonlat[:, 1]), np.array([iso in known for iso in isos])


def view(zoom: list[float] | None = None) -> tuple[float, float, float, float]:
    """The map's left, right, bottom and top, projected: the world's, or, with `zoom`,
    those of its west, east, south and north, in degrees, widened or heightened to the
    world's shape."""
    x0, x1 = -2.75, 2.75
    y0, y1 = equal_earth(np.array([0]), np.array([SOUTH]))[0, 1] - 0.03, 1.33
    if not zoom:
        return x0, x1, y0, y1
    west, east, south, north = zoom
    corners = equal_earth(np.array([west, west, east, east, (west + east) / 2, (west + east) / 2]),
                          np.array([south, north, south, north, south, north]))
    (left, low), (right, high) = corners.min(axis=0), corners.max(axis=0)
    shape = (y1 - y0) / (x1 - x0)  # the map's height over its width
    wide = max(right - left, (high - low) / shape)
    middle_x, middle_y = (left + right) / 2, (low + high) / 2
    return middle_x - wide / 2, middle_x + wide / 2, middle_y - wide * shape / 2, middle_y + wide * shape / 2


def frame(title: str, subtitle: str, zoom: list[float] | None = None
          ) -> tuple[plt.Figure, plt.Axes, plt.Polygon, plt.Axes]:
    """A figure headed by the title and, unless it's "", the subtitle, wrapped to its
    width, with the land in the globe's edge under them, or the part of it in view(zoom),
    and the legend's axes, 15 x 2 in its data's units, at the bottom: (figure, map,
    globe's edge, legend)."""
    heading = textwrap.wrap(title, int((FIG_WIDTH - 0.6) * 72 / (14 * 0.58)))
    subheading = textwrap.wrap(subtitle, int((FIG_WIDTH - 0.6) * 72 / (9 * 0.58)))
    # inches: the title's lines, and the subtitle's under them, if there is one
    header = 0.25 + 0.3 * len(heading) + (0.08 + 0.19 * len(subheading) if subheading else 0) + 0.15
    x0, x1, y0, y1 = view()
    map_height = (FIG_WIDTH - 0.4) * (y1 - y0) / (x1 - x0)
    fig_height = header + map_height + LEGEND_HEIGHT
    fig = plt.figure(figsize=(FIG_WIDTH, fig_height), facecolor=pf.SURFACE)
    fig.text(0.3 / FIG_WIDTH, 1 - 0.25 / fig_height, "\n".join(heading), ha="left", va="top", color=pf.INK,
             fontsize=14, linespacing=1.25)
    if subheading:
        fig.text(0.3 / FIG_WIDTH, 1 - (0.25 + 0.3 * len(heading) + 0.08) / fig_height, "\n".join(subheading),
                 ha="left", va="top", color=pf.INK_2, fontsize=9, linespacing=1.4)
    ax = fig.add_axes((0.2 / FIG_WIDTH, LEGEND_HEIGHT / fig_height, (FIG_WIDTH - 0.4) / FIG_WIDTH,
                       map_height / fig_height))
    ax.set_aspect("equal")
    x0, x1, y0, y1 = view(zoom)
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.axis("off")
    edge = np.concatenate([equal_earth(np.full(90, 180), np.linspace(SOUTH, 90, 90)),
                           equal_earth(np.full(90, -180), np.linspace(90, SOUTH, 90))])
    globe = ax.add_patch(plt.Polygon(edge, facecolor="none", edgecolor=pf.GRID, linewidth=0.8, zorder=0))
    ax.add_collection(PolyCollection(land(), facecolors=LAND, edgecolors="none", zorder=0))
    legend = fig.add_axes((0.3 / FIG_WIDTH, 0.1 / fig_height, (FIG_WIDTH - 0.6) / FIG_WIDTH,
                           (LEGEND_HEIGHT - 0.2) / fig_height))
    legend.axis("off")
    legend.set_xlim(0, 15)
    legend.set_ylim(0, 2)
    return fig, ax, globe, legend


def draw_arcs(ax: plt.Axes, globe: plt.Polygon, arcs: list[tuple[np.ndarray, np.ndarray, float, str]],
              alpha: tuple[float, float] = (0.08, 0.88), taper: float = 0.15, bow: float = BOW[0],
              dash: float | None = None) -> None:
    """Draw the (start, end, width, colour) arcs, in turn, within the globe's edge,
    each a quadratic Bézier curve bowing to the left of its direction by `bow` of its
    length, as segments that grow from `taper` of its width, in points, to all of it,
    and from the first to the second opacity in `alpha` along it. With `dash`, each is
    dashed, in dashes and gaps that long on the unit sphere's scale."""
    points = POINTS if dash is None else 6 * POINTS
    s = np.linspace(0, 1, points)[:, None]
    along = (s[:-1, 0] + s[1:, 0]) / 2
    segments, widths, colours = [], [], []
    for p0, p1, width, colour in arcs:
        d = p1 - p0
        length = np.hypot(*d)
        control = (p0 + p1) / 2 + min(bow, BOW[1] / length) * np.array([-d[1], d[0]])  # to the left of the direction
        path = (1 - s) ** 2 * p0 + 2 * (1 - s) * s * control + s ** 2 * p1
        rgba = np.tile(plt.matplotlib.colors.to_rgba(colour), (points - 1, 1))
        rgba[:, 3] = alpha[0] + (alpha[1] - alpha[0]) * along ** 1.5
        pieces = np.stack([path[:-1], path[1:]], axis=1)
        wide = np.maximum(0.25, width * (taper + (1 - taper) * along ** 0.8))
        if dash is not None:
            steps = np.hypot(*(path[1:] - path[:-1]).T)
            shown = (np.cumsum(steps) - steps / 2) // dash % 2 == 0
            pieces, wide, rgba = pieces[shown], wide[shown], rgba[shown]
        segments.extend(pieces)
        widths.extend(wide)
        colours.extend(rgba)
    # butt caps: round ones overlap, and show as beads along the wider, translucent arcs
    lines = ax.add_collection(LineCollection(segments, linewidths=widths, colors=colours, capstyle="butt", zorder=1))
    lines.set_clip_path(globe)


def label(ax: plt.Axes, points: list[tuple[np.ndarray, float, str, str]], dots: list[tuple[np.ndarray, float]],
          clear: int | None = None) -> None:
    """Write the (position, radius of its dot in points, text, weight) labels in turn, each at the first of PLACES
    around its dot that stays in the map and overlaps none of `dots` (position, radius) nor an earlier label, or else
    at the one that overlaps them least. With `clear`, those that overlap anywhere are left out, and the first
    `clear` that don't are written (the first point's, whatever it overlaps)."""
    renderer = ax.figure.canvas.get_renderer()
    panel, pixels = ax.get_window_extent(renderer), ax.figure.dpi / 72
    taken = []
    for position, radius in dots:
        x, y = ax.transData.transform(position)
        taken.append(Bbox.from_bounds(x - radius * pixels, y - radius * pixels, *[2 * radius * pixels] * 2))

    def overlap(box: Bbox) -> float:
        """The area of `box` that's off the map or over what's taken, in pixels²."""
        areas = [inside.width * inside.height for t in taken if (inside := Bbox.intersection(box, t))]
        shown = Bbox.intersection(box, panel)
        return sum(areas) + box.width * box.height - (shown.width * shown.height if shown else 0)

    halo = [patheffects.withStroke(linewidth=2.5, foreground=pf.SURFACE)]

    def write(position: np.ndarray, radius: float, text: str, weight: str, place: tuple) -> Annotation:
        """The label at one of PLACES."""
        dx, dy, ha, va = place
        return ax.annotate(text, position, xytext=((radius + 2.5) * dx, (radius + 2.5) * dy),
                           textcoords="offset points", ha=ha, va=va, fontsize=8.5, fontweight=weight, color=pf.INK,
                           path_effects=halo, zorder=4)

    written = 0
    for number, point in enumerate(points):
        if clear is not None and written > clear:  # the first point's isn't counted
            break
        overlaps = []
        for place in PLACES:
            note = write(*point, place)
            overlaps.append(overlap(note.get_window_extent(renderer)))
            note.remove()
            if overlaps[-1] < 1:
                break
        if clear is not None and number and min(overlaps) >= 1:
            continue
        taken.append(write(*point, PLACES[int(np.argmin(overlaps))]).get_window_extent(renderer))
        written += 1
