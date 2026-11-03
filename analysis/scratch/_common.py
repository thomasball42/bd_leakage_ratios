# -*- coding: utf-8 -*-
"""
What the scratch plots share: the paths, the country attributes written by
country_attributes.py, the groupings, and the style (the scatter plots' own, from
figures/_plot_funcs.py). Figures go to {figs_dir}/{run_name}/scratch/.
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


def save(fig: plt.Figure, name: str, config: Path = _run_paths.DEFAULT_CONFIG) -> None:
    path = out_dir(config) / name
    fig.savefig(path, dpi=200, facecolor=pf.SURFACE)
    plt.close(fig)
    print(f"Wrote {path}")
