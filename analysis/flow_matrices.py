# -*- coding: utf-8 -*-
"""
The leakage flow matrices written by build_flow_matrices.py, one item at a time:

    import flow_matrices as fm
    beef = fm.load("Meat of cattle with the bone, fresh or chilled")   # or by item code, fm.load(867)
    beef["sizes"]                                    # the run's displacement sizes
    fm.matrix(beef, "bd")                            # the ratio matrix at the smallest (marginal) size
    fm.matrix(beef, "land_use_m2", "net", size=0.01)
    fm.matrix(beef, "bd", "per_kg", percentile=95)
    fm.leakage_ratio(beef, "bd")                     # each country's leakage ratio, with its percentiles

Rows are the displacing countries C, columns the locations where the impacts are
(ISO3, or the FAOSTAT code where there's none): where the land is, so an animal
product's feed land is where the feed is grown, and its pasture where the animal
is. `kind` is one of:
    per_kg  the impact leaked to the location, per kg displaced in C
    spared  the impact spared at the location, per kg displaced in C (for an animal
            product, C's own pasture and its feed's land, wherever grown)
    net     leaked - spared at the location, per kg displaced in C
    ratio   leaked to the location over all C spares; each row sums to C's
            leakage ratio (leaked / spared, the run's {metric}_ratio)
    share   the location's share of all C leaks; each row sums to 1
for a `metric` in METRICS: land use, extinctions and carbon opportunity cost
(extensified, located where the land is), or production GHG (linear, located
where the item is produced). A cell is 0 where nothing changes at the location,
and NaN where the run has no extensified result.

The C column holds what changes in C: in `spared`, C's own land, and in `per_kg`,
the land of the part of its displaced production C replaces itself (when the run's
replacement.exclude_self is false), which the run counts as leaked, and of any
feed grown in C for the other replacing producers. `exclude_self=True` leaves the
C column out, so ratio rows sum to the leakage elsewhere and share rows are shares
of it.

`percentile` gives the cells' percentiles over the run's draws, for per_kg and
spared. The ratio, share and net cells have none, as they combine values whose
draws aren't in the raw outputs; their rows' sums, the leakage ratios, do
(leakage_ratio). So do the per_kg and spared rows' sums, what C leaks and spares
in all (totals), whose percentiles, unlike sums of the cells', are their own. m["replaced_share"] is the share of C's displaced production that
each location replaces as a producer, the same at every size; frame() labels it,
or any other countries x locations array.

load("portfolio") gives the portfolio matrix written by build_portfolio_matrices.py,
in the same form: each country's row per kg of its production portfolio displaced,
its row of each item it produces weighted by its production of the item, so a
country that produces only wheat has the wheat matrix's row. It has the leakage
ratios, but no percentiles; its weights and covered_share are documented there.
"""

from pathlib import Path

import numpy as np
import pandas as pd

import _run_paths

METRICS = ["land_use_m2", "bd", "ghg_coc_kgco2e", "ghg_prod_kgco2e"]
EXTENSIFIED = METRICS[:3]  # with percentiles, and leakage ratios, in the run's outputs
KINDS = ["per_kg", "spared", "net", "ratio", "share"]
SUBDIR = "flow_matrices"  # in the run's processed folder
INDEX = "items.csv"  # in SUBDIR, listing the items' files
PORTFOLIO = "portfolio"  # load()'s name for the portfolio matrix, {PORTFOLIO}.npz in SUBDIR


def flow_dir(config_path: Path = _run_paths.DEFAULT_CONFIG) -> Path:
    """The folder of the flow matrices of the run in `config_path`."""
    return _run_paths.run_paths(config_path)[2] / SUBDIR


def items(config_path: Path = _run_paths.DEFAULT_CONFIG) -> pd.DataFrame:
    """The items with flow matrices: their code, name and file, and their numbers of
    displacing countries and of locations."""
    return pd.read_csv(flow_dir(config_path) / INDEX)


def load(item: int | str, config_path: Path = _run_paths.DEFAULT_CONFIG) -> dict[str, np.ndarray]:
    """One item's flow matrices, by its item code or name (in any case), or the
    portfolio matrix, for "portfolio"."""
    directory = flow_dir(config_path)
    if isinstance(item, str) and item.lower() == PORTFOLIO:
        path = directory / f"{PORTFOLIO}.npz"
    else:
        index = pd.read_csv(directory / INDEX)
        if isinstance(item, (int, np.integer)):
            match = index[index["item_code"] == item]
        else:
            match = index[index["item_name"].str.lower() == item.lower()]
        if len(match) != 1:
            raise KeyError(f"No flow matrices for item {item!r} in {directory / INDEX}")
        path = directory / match["file"].iloc[0]
    with np.load(path) as f:
        return {key: f[key] for key in f.files}


def size_index(m: dict, size: float | None = None) -> int:
    """The position of the run's displacement size nearest `size` (on a log scale),
    or for None the smallest, the nearest to marginal."""
    if size is None:
        return 0
    return int(np.argmin(np.abs(np.log(m["sizes"]) - np.log(size))))


def self_cells(m: dict) -> np.ndarray:
    """(countries x locations) mask of each country's own column."""
    return m["countries"][:, None] == m["locations"][None, :]


def cube(m: dict, metric: str, kind: str = "ratio", exclude_self: bool = False,
         percentile: float | None = None) -> np.ndarray:
    """(sizes x countries x locations) array of `kind` for `metric`, central or at
    a `percentile` (see the module docstring)."""
    if metric not in METRICS:
        raise ValueError(f"metric must be one of {METRICS}, not {metric!r}")
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}, not {kind!r}")
    if percentile is None:
        suffix = ""
    elif kind in ["per_kg", "spared"] and f"{metric}_{'spared' if kind == 'spared' else 'leaked'}_p{percentile:g}" in m:
        suffix = f"_p{percentile:g}"
    else:
        raise ValueError(f"No percentile {percentile} of {metric} {kind}; there are percentiles of per_kg and spared "
                         f"for {EXTENSIFIED}, at the run's percentiles")
    if kind == "spared":
        values = m[f"{metric}_spared{suffix}"].astype(np.float64)
    else:
        values = m[f"{metric}_leaked{suffix}"].astype(np.float64)
        if kind == "net":
            values -= m[f"{metric}_spared"]
    if exclude_self:
        values[:, self_cells(m)] = 0
    with np.errstate(divide="ignore", invalid="ignore"):
        if kind == "ratio":
            spared = m[f"{metric}_spared_total"]
            values /= np.where(spared != 0, spared, np.nan)[..., None]
        elif kind == "share":
            leaked = np.where(np.isnan(values).all(axis=-1), np.nan, np.nansum(values, axis=-1))
            values /= np.where(leaked != 0, leaked, np.nan)[..., None]
    return values


def frame(m: dict, array: np.ndarray) -> pd.DataFrame:
    """A (countries x locations) array, labelled."""
    return pd.DataFrame(array, index=pd.Index(m["country_isos"], name="displacing"),
                        columns=pd.Index(m["location_isos"], name="location"))


def matrix(m: dict, metric: str, kind: str = "ratio", size: float | None = None, exclude_self: bool = False,
           percentile: float | None = None) -> pd.DataFrame:
    """The countries x locations matrix of `kind` for `metric` at the run's
    displacement size nearest `size` (default: the smallest), central or at a
    `percentile`."""
    return frame(m, cube(m, metric, kind, exclude_self, percentile)[size_index(m, size)])


def leakage_ratio(m: dict, metric: str, size: float | None = None) -> pd.DataFrame:
    """Each displacing country's leakage ratio (leaked / spared) for an extensified
    `metric`, central and its percentiles over the run's draws, at the run's
    displacement size nearest `size` (default: the smallest)."""
    if metric not in EXTENSIFIED:
        raise ValueError(f"metric must be one of {EXTENSIFIED}, not {metric!r}")
    i = size_index(m, size)
    columns = [key for key in m if key == f"{metric}_ratio" or key.startswith(f"{metric}_ratio_p")]
    return pd.DataFrame({key.removeprefix(f"{metric}_"): m[key][i] for key in columns},
                        index=pd.Index(m["country_isos"], name="displacing"))


def totals(m: dict, metric: str, kind: str = "leaked", percentile: float | None = None) -> np.ndarray:
    """(sizes x countries) array of all C leaks or spares (`kind`) of `metric`,
    per kg displaced, the rows' sums of the per_kg or spared matrix, central or
    at a `percentile` over the run's draws. Unlike the cells', these percentiles
    are the totals' own."""
    if kind not in ["leaked", "spared"]:
        raise ValueError(f"kind must be leaked or spared, not {kind!r}")
    key = f"{metric}_{kind}_total" + ("" if percentile is None else f"_p{percentile:g}")
    if key not in m:
        raise ValueError(f"No {key} in these flow matrices: the totals' percentiles are for {EXTENSIFIED}, at the "
                         "run's percentiles, and not for the portfolio; or the matrices were built before the totals "
                         "were added, and need rebuilding")
    return m[key]
