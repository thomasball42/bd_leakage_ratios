# -*- coding: utf-8 -*-
"""
The portfolio flow matrix of a bd_leakage_ratios run, from its items' flow
matrices: `python build_portfolio_matrices.py [config.json]`, with the run's config
(default: the repository's config.json) or its record, once build_flow_matrices.py
has written them.

Each displacing country C's row is per kg of its production portfolio displaced:
its row of every item it produces, weighted by its FLMP production of the item,

    P[size, C, location] = sum_i Q_i(C) M_i[size, C, location] / sum_i Q_i(C)

over the items with extensified results at that size (the others are left out and
the weights renormalised; covered_share is the share of C's production kept). So a
country that produces only wheat has the wheat matrix's row. The weights are gross
production, so feed grown in C for C's own animals counts both as the crop and in
the animal products' feed.

With the run's displacement.unit "share", item i displaces size * Q_i(C) kg, so
this is exactly the leakage of C displacing the same share of everything it
produces, the sum of the items' scenarios, at every size. With "kg", it weights
each item's per-kg values at the same kg, which is the portfolio's only at
marginal sizes. The items' scenarios are separate, so land sources two items share
aren't netted between them.

Written to the run folder's processed/flow_matrices/, as portfolio.npz, with the
keys of an item's file, so flow_matrices.load("portfolio") and its functions work
on it, and with items, item_names, weights (countries x items, each item's share of
C's production) and covered_share (sizes x countries). It has no percentiles, as a
sum's can't be had from its terms', and none of an item's location metadata or
current land use. It's checked against leakage_summary.csv: its rows must sum to
C's leaked and spared amounts per kg displaced, weighted the same way.
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

import _run_paths
import flow_matrices
from flow_matrices import EXTENSIFIED, METRICS

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("config", nargs="?", type=Path, default=_run_paths.DEFAULT_CONFIG,
                    help="the run's config.json, or its record (default: the repository's config.json)")
args = parser.parse_args()

config, RAW_DIR, PROCESSED_DIR, DATA_DIR = _run_paths.run_paths(args.config)
FLOW_DIR = PROCESSED_DIR / flow_matrices.SUBDIR
SIZE_UNIT: str = config["displacement"]["unit"]
NAME = "Production portfolio"  # its item_name; its item_code is 0
TOLERANCE = 1e-6  # the largest relative difference from the summary the checks allow
LOW_COVERAGE = 0.9  # countries with less of their production in the portfolio are listed
CUBES = [f"{metric}_{kind}" for metric in METRICS for kind in ["leaked", "spared"]]

if SIZE_UNIT != "share":
    print(f"Warning: the run's displacement.unit is {SIZE_UNIT!r}, so the portfolio weights each item's per-kg values "
          f"at the same kg displaced, which is a portfolio's only at marginal sizes")


def read(file: str, keys: list[str]) -> dict[str, np.ndarray]:
    """`keys` of one item's flow matrices file."""
    with np.load(FLOW_DIR / file) as f:
        return {key: f[key] for key in keys}


def relative_error(row_sums: np.ndarray, expected: np.ndarray, scale: np.ndarray) -> float:
    """The largest difference between `row_sums` and `expected`, relative to
    `scale`, where both are finite."""
    finite = np.isfinite(row_sums) & np.isfinite(expected) & (scale > 0)
    return float(np.max(np.abs(row_sums - expected)[finite] / scale[finite], initial=0))


index = flow_matrices.items(args.config)
heads = [read(file, ["sizes", "countries", "country_isos", "locations", "location_isos"])
         for file in tqdm(index["file"], desc="Reading items' countries")]
SIZES = heads[0]["sizes"]
for item_code, head in zip(index["item_code"], heads):
    if not np.array_equal(head["sizes"], SIZES):
        raise ValueError(f"Item {item_code}'s sizes differ from item {index['item_code'].iloc[0]}'s")
countries = np.unique(np.concatenate([head["countries"] for head in heads]))
locations = np.unique(np.concatenate([head["locations"] for head in heads]))
iso_of = {}
for head in heads:
    iso_of.update(zip(head["countries"], head["country_isos"]))
    iso_of.update(zip(head["locations"], head["location_isos"]))
print(f"{FLOW_DIR}: {len(index)} items, {len(countries)} countries, {len(locations)} locations and {len(SIZES)} sizes")

# the items' values times the kg each country produces of them, summed over the items in its portfolio at each size
shape = (len(SIZES), len(countries), len(locations))
totals = {cube: np.zeros(shape) for cube in CUBES}
scales = {cube: np.zeros(shape[:2]) for cube in CUBES}  # the sizes of the cells summed in each row, for the checks
spared_totals = {metric: np.zeros(shape[:2]) for metric in METRICS}
replaced = np.zeros(shape[1:])
covered_kg = np.zeros(shape[:2])  # the production in each country's portfolio at each size
displaced_kg = np.zeros(shape[:2])
production = np.zeros((len(countries), len(index)))  # each country's production of each item
for j, (file, head) in enumerate(tqdm(list(zip(index["file"], heads)), desc="Adding items")):
    m = read(file, ["country_production_kg", "displaced_kg", "replaced_share", *CUBES,
                    *(f"{metric}_spared_total" for metric in METRICS)])
    c, loc = np.searchsorted(countries, head["countries"]), np.searchsorted(locations, head["locations"])
    cells = np.ix_(np.arange(len(SIZES)), c, loc)
    q = np.nan_to_num(m["country_production_kg"])
    production[c, j] = q
    # in the portfolio where the item has extensified results (sizes x the item's countries)
    included = np.isfinite(np.stack([m[f"{metric}_spared_total"] for metric in EXTENSIFIED])).all(axis=0)
    weight = np.where(included, q, 0.0)
    covered_kg[:, c] += weight
    displaced_kg[:, c] += np.where(included, m["displaced_kg"], 0)
    replaced[np.ix_(c, loc)] +=np.where(included[0][:, None], weight[0][:, None] * m["replaced_share"], 0)
    for cube in CUBES:
        totals[cube][cells] += np.where(included[..., None], weight[..., None] * m[cube], 0)
        scales[cube][:, c] += np.where(included, weight * np.abs(m[cube]).sum(axis=-1), 0)
    for metric in METRICS:
        spared_totals[metric][:, c] += np.where(included, weight * m[f"{metric}_spared_total"], 0)

country_kg = production.sum(axis=1)
per_kg = 1 / np.where(covered_kg > 0, covered_kg, np.nan)
arrays = {
    "item_code": np.array(0), "item_name": np.array(NAME),
    "run_name": np.array(config["run"]["run_name"]), "size_unit": np.array(SIZE_UNIT),
    "sizes": SIZES, "countries": countries, "country_isos": np.array([iso_of[code] for code in countries]),
    "locations": locations, "location_isos": np.array([iso_of[code] for code in locations]),
    # the kg displaced: the items' at the same share, or the size in kg
    "displaced_kg": (np.where(covered_kg > 0, displaced_kg, np.nan) if SIZE_UNIT == "share"
                     else np.where(covered_kg > 0, SIZES[:, None], np.nan)),
    "country_production_kg": country_kg,
    "replaced_share": (replaced * per_kg[0][:, None]).astype(np.float32),
    "items": index["item_code"].to_numpy(), "item_names": index["item_name"].to_numpy(str),
    "weights": production / country_kg[:, None],
    "covered_share": covered_kg / country_kg,
}
for cube in CUBES:
    arrays[cube] = (totals[cube] * per_kg[..., None]).astype(np.float32)
for metric in METRICS:
    arrays[f"{metric}_spared_total"] = spared_totals[metric] * per_kg
    arrays[f"{metric}_leaked_total"] = totals[f"{metric}_leaked"].sum(axis=-1) * per_kg
    if metric in EXTENSIFIED:
        spared = spared_totals[metric]
        arrays[f"{metric}_ratio"] = totals[f"{metric}_leaked"].sum(axis=-1) / np.where(spared != 0, spared, np.nan)

# check the rows against the summary's leaked and spared amounts per kg displaced, weighted by production, relative to
# the sizes of the items' cells they sum, as items' cells of opposite signs can cancel
summary = pd.read_csv(RAW_DIR / "leakage_summary.csv",
                      usecols=["COUNTRY_OF_INTEREST_CODE", "ItemT_Code", "size", "displaced_kg", "production_kg",
                               *(f"{metric}_{kind}" for metric in METRICS for kind in ["leaked", "spared"])])
summary = summary[summary[[f"{metric}_spared" for metric in EXTENSIFIED]].notna().all(axis=1)]
at = (np.searchsorted(SIZES, summary["size"]), np.searchsorted(countries, summary["COUNTRY_OF_INTEREST_CODE"]))
if (SIZES[np.minimum(at[0], len(SIZES) - 1)] != summary["size"]).any():
    raise ValueError("The summary's sizes differ from the flow matrices'")
q = np.nan_to_num(summary["production_kg"].to_numpy())
expected_kg = np.zeros(shape[:2])
np.add.at(expected_kg, at, q)
checks = {"covered_kg": relative_error(covered_kg, expected_kg, covered_kg)}
for metric in METRICS:
    for kind in ["leaked", "spared"]:
        expected = np.zeros(shape[:2])
        np.add.at(expected, at, q * summary[f"{metric}_{kind}"].to_numpy() / summary["displaced_kg"].to_numpy())
        expected /= np.where(expected_kg > 0, expected_kg, np.nan)
        cells = arrays[f"{metric}_{kind}"].astype(np.float64)  # spared positive, as in the summary
        checks[f"{metric}_{kind}"] = relative_error(cells.sum(axis=-1), expected, scales[f"{metric}_{kind}"] * per_kg)
    checks[f"{metric}_spared_total"] = relative_error(arrays[f"{metric}_spared_total"],
                                                      arrays[f"{metric}_spared"].astype(np.float64).sum(axis=-1),
                                                      scales[f"{metric}_spared"] * per_kg)

np.savez_compressed(FLOW_DIR / f"{flow_matrices.PORTFOLIO}.npz", **arrays)
print("Largest relative differences from the summary: "
      + ", ".join(f"{check} {error:.1e}" for check, error in checks.items()))
failed = {check: error for check, error in checks.items() if error > TOLERANCE}
if failed:
    print(f"Warning: {len(failed)} checks differ from the summary by more than {TOLERANCE}: {failed}")
low = arrays["covered_share"][0] < LOW_COVERAGE
if low.any():
    print(f"{low.sum()} countries have less than {LOW_COVERAGE:.0%} of their production in the portfolio, the rest "
          "having no extensified results: "
          + ", ".join(f"{iso} {share:.0%}" for iso, share in zip(arrays["country_isos"][low],
                                                                  arrays["covered_share"][0][low])))
print(f"Wrote the portfolio flow matrix of {len(countries)} countries to {FLOW_DIR / flow_matrices.PORTFOLIO}.npz")
