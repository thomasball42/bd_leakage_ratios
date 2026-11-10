# -*- coding: utf-8 -*-
"""
What the scratch plots share: the paths, the country attributes written by
country_attributes.py, the groupings, and the style (the scatter plots' own, from
figures/_plot_funcs.py). Figures go to {figs_dir}/{run_name}/scratch/, in a
subfolder per kind (FOLDERS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRATCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRATCH_DIR.parent))  # the analysis modules
sys.path.insert(0, str(SCRATCH_DIR.parent / "figures"))  # _plot_funcs

import _plot_funcs as pf  # noqa: E402
import _run_paths  # noqa: E402
import flow_matrices as fm  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

plt = pf.plt
DATA_DIR = SCRATCH_DIR / "data"
ATTRIBUTES_PATH = DATA_DIR / "country_attributes.csv"
METRICS = fm.EXTENSIFIED  # land use, extinctions, carbon opportunity cost
SHORT = {"land_use_m2": "Land use", "bd": "Extinctions", "ghg_coc_kgco2e": "Carbon opp. cost",
         "ghg_prod_kgco2e": "Production GHG"}

# the World Bank's groupings, in the order they're shown
GROUPINGS = {
    "income": ("income_group", ["High income", "Upper middle income", "Lower middle income", "Low income"]),
    "region": ("wb_region", ["North America", "Europe & Central Asia", "East Asia & Pacific",
                             "Latin America & Caribbean", "Middle East, North Africa, Afghanistan & Pakistan",
                             "South Asia", "Sub-Saharan Africa"]),
}
GROUP_LABELS = {"Middle East, North Africa, Afghanistan & Pakistan": "Middle East & N. Africa",
                "Latin America & Caribbean": "Latin America & Carib.", "Upper middle income": "Upper middle",
                "Lower middle income": "Lower middle", "High income": "High", "Low income": "Low"}

# the reference palette (see _plot_funcs): categorical slots 1 and 2, muted ink, the blue ramp, and the diverging
# blue <-> red pair around a gray midpoint
SPARED, LEAKED, MUTED = pf.SERIES, "#eb6834", "#898781"
BLUES = LinearSegmentedColormap.from_list("blues", ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"])
DIVERGING = LinearSegmentedColormap.from_list("blue_red", ["#184f95", "#6da7ec", "#f0efec", "#ef8a89", "#a52a2a"])
# the World Bank regions in their order around the globe, east from the Americas, as flow_chord.py's ring shows
# them, and their colours: the reference palette's first seven categorical slots in the order 1, 2, 7, 4, 5, 6, 3,
# which keeps every pair of neighbours on the ring, the last and first too, apart in colour-blind (OKLab ΔE ≥ 15)
# and normal vision. The countries with no region are NO_REGION's, in muted ink.
REGION_COLOURS = {
    "North America": "#2a78d6",
    "Latin America & Caribbean": "#eb6834",
    "Sub-Saharan Africa": "#6250d6",
    "Middle East, North Africa, Afghanistan & Pakistan": "#eda100",
    "Europe & Central Asia": "#e87ba4",
    "South Asia": "#008300",
    "East Asia & Pacific": "#1baf7a",
}
NO_REGION = "No region"


# the subfolders of out_dir() the figures go in, and what goes in each
FLOWS, PROFILES, VS_ATTRIBUTES = "flows", "profiles", "vs_attributes"
FOLDERS = {
    FLOWS: "the leakage between groups and between countries (group_flows.py, country_flows.py), and every "
           "country's leakage abroad at once (flow_chord.py, flow_map.py)",
    PROFILES: "where one country's leakage goes (country_profile.py)",
    VS_ATTRIBUTES: "the countries' leakage against their GDP per capita and self-sufficiency (leakage_vs_axes.py, "
                   "destination_gdp.py)",
}


def out_dir(config: Path = _run_paths.DEFAULT_CONFIG) -> Path:
    """The scratch figures folder, {figs_dir}/{run_name}/scratch."""
    path = (_run_paths.figs_dir(config) / "scratch").resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def attributes() -> pd.DataFrame:
    """The country attributes, by ISO3: name, World Bank region and income group,
    GDP per capita and self-sufficiency (see country_attributes.py)."""
    if not ATTRIBUTES_PATH.exists():
        sys.exit(f"No {ATTRIBUTES_PATH}: run country_attributes.py first")
    return pd.read_csv(ATTRIBUTES_PATH, index_col="ISO3", keep_default_na=False, na_values=[""])


def load(item: str, config: Path = _run_paths.DEFAULT_CONFIG) -> dict[str, np.ndarray]:
    """The flow matrices of an item code or name, or the portfolio."""
    return fm.load(int(item) if item.isdigit() else item, config)


def is_portfolio(m: dict) -> bool:
    return "weights" in m  # only the portfolio has its items' weights


def item_label(m: dict) -> str:
    """The item's name, or the production portfolio."""
    return m["item_name"].item()


def stem(m: dict) -> str:
    """The item's part of a figure's file name."""
    return fm.PORTFOLIO if is_portfolio(m) else str(m["item_code"].item())


def flows(m: dict, metric: str, i: int, unit_of: dict[str, str], units: list[str]) -> tuple[np.ndarray, np.ndarray]:
    """What each country's displacement leaks to and spares in each location at
    size position i, weighted by the country's production (as if every country
    displaced the same share of its production, each in its own scenario), and
    summed into `units`: (units x units) leaked and spared, the rows displacing and
    the columns where the impact is. A country's or location's unit is
    unit_of[ISO3]; those with none, or one not in `units`, are left out, and the
    share of the impact that leaves out is printed."""
    weight = np.nan_to_num(m["country_production_kg"])[:, None]
    index = {unit: k for k, unit in enumerate(units)}
    rows = np.array([index.get(unit_of.get(iso), -1) for iso in m["country_isos"]])
    cols = np.array([index.get(unit_of.get(iso), -1) for iso in m["location_isos"]])
    cells = np.ix_(rows >= 0, cols >= 0)
    out = []
    for kind in ["leaked", "spared"]:
        values = np.nan_to_num(m[f"{metric}_{kind}"][i].astype(float)) * weight
        print(f"{kind}: {1 - np.abs(values[cells]).sum() / np.abs(values).sum():.2%} of the impact is in or from "
              "countries with no unit, and left out")
        summed = np.zeros((len(units), len(units)))
        np.add.at(summed, (rows[rows >= 0][:, None], cols[cols >= 0][None, :]), values[cells])
        out.append(summed)
    missing = set(m["country_isos"][rows < 0]) | set(m["location_isos"][cols < 0])
    if missing:
        print(f"No unit: {', '.join(sorted(missing))}")
    return out[0], out[1]


def cross_border(m: dict, metric: str, i: int) -> tuple[list[str], np.ndarray, float]:
    """Every country's leakage to every other at size position i, weighted by its
    production as in flows(): (isos, L, home), with L[A, B] what A's displacement
    leaks to B, its diagonal, what stays in A, 0, and home that diagonal's share of
    all the leakage. Negative cells (from negative betas) are set to 0, and the
    share of the leakage they hold printed."""
    isos = sorted(set(m["country_isos"]) | set(m["location_isos"]))
    leaked, _ = flows(m, metric, i, {iso: iso for iso in isos}, isos)
    home = np.trace(leaked) / leaked.sum()
    np.fill_diagonal(leaked, 0)
    negative = -leaked[leaked < 0].sum()
    if negative:
        print(f"Negative cross-border cells, set to 0: {negative / np.abs(leaked).sum():.2%} of their absolute sum")
    return isos, leaked.clip(0), home


def region_of(isos: list[str]) -> dict[str, str]:
    """Each ISO3's World Bank region, a key of REGION_COLOURS, or NO_REGION."""
    region = attributes()["wb_region"].dropna().to_dict()
    return {iso: region[iso] if region.get(iso) in REGION_COLOURS else NO_REGION for iso in isos}


def production(m: dict, metric: str, i: int, unit_of: dict[str, str], units: list[str]) -> np.ndarray:
    """Each unit's production, in kg, of its countries with a result for `metric`
    at size position i: flows()' rows over it are per kg the unit displaces (the
    production-weighted mean of its countries' rows)."""
    index = {unit: k for k, unit in enumerate(units)}
    rows = np.array([index.get(unit_of.get(iso), -1) for iso in m["country_isos"]])
    weight = np.where(np.isnan(m[f"{metric}_leaked"][i].astype(float)).all(axis=1), 0,
                      np.nan_to_num(m["country_production_kg"]))
    return np.bincount(rows[rows >= 0], weight[rows >= 0], minlength=len(units))


# the flow panels: their titles, and what their values are a % of, for a displacing {unit}
FLOW_PANELS = {
    "lands": ("Where the leakage lands", "% of all the displacing {unit} leaks"),
    "net": ("Net change where it lands", "% of all the displacing {unit} spares"),
    "direction": ("Net direction, A → B minus B → A", "% of all that's spared worldwide"),
}


def flow_panels(leaked: np.ndarray, spared: np.ndarray) -> dict[str, np.ndarray]:
    """From flows()' leaked and spared, the values of FLOW_PANELS:
        lands      leaked[A, B] / leaked[A, :]: each row sums to 100%
        net        (leaked - spared)[A, B] / spared[A, :], as % of all A spares:
                   each row sums to A's leakage ratio - 1, and the diagonal is
                   the change at home
        direction  N[A, B] - N[B, A], with N the net change in B from A's
                   displacement, as % of all that's spared worldwide: positive
                   where more net impact moves from A to B than from B to A
    Rows that leak (for lands) or spare (for net) nothing or less are NaN."""
    net = leaked - spared
    leaked_total, spared_total = leaked.sum(1, keepdims=True), spared.sum(1, keepdims=True)
    with np.errstate(divide="ignore", invalid="ignore"):
        return {
            "lands": np.where(leaked_total > 0, leaked / leaked_total, np.nan),
            "net": np.where(spared_total > 0, net / spared_total, np.nan),
            "direction": np.where(np.eye(len(net), dtype=bool), np.nan, net - net.T) / spared.sum(),
        }


def new_axes(ax: plt.Axes, grid: str = "both") -> plt.Axes:
    """Style an axes as the scatter plots are: no spines, a hairline grid, and quiet ticks."""
    ax.set_facecolor(pf.SURFACE)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(color=pf.GRID, linewidth=0.8, axis=grid)
    ax.set_axisbelow(True)
    ax.tick_params(colors=pf.INK_2, length=0, labelsize=8)
    return ax


def save(fig: plt.Figure, name: str, config: Path = _run_paths.DEFAULT_CONFIG, *, folder: str) -> None:
    """Write the figure to `name` in out_dir()'s `folder`, one of FOLDERS, and close it."""
    if folder not in FOLDERS:
        raise ValueError(f"folder must be one of {list(FOLDERS)}, not {folder!r}")
    path = out_dir(config) / folder / name
    path.parent.mkdir(exist_ok=True)
    fig.savefig(path, dpi=200, facecolor=pf.SURFACE)
    plt.close(fig)
    print(f"Wrote {path}")
