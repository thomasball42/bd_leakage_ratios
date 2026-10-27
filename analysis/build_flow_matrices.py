# -*- coding: utf-8 -*-
"""
Leakage flow matrices of a bd_leakage_ratios run, one file per item, from the run's
raw outputs: `python build_flow_matrices.py [config.json] [--workers N]`, with the
run's config (default: the repository's config.json) or its record, and N processes
reading the files and building the items at once.

For each displacement size, displacing country C (rows) and location (columns),
from the rows of the {ISO}_leakage.csv files, per kg displaced in C: the
extensified land use, extinctions and carbon opportunity cost leaked to (the
replacement rows) and spared at (the displaced rows) each location, with their
percentiles over the draws, and production GHG; and the share of C's displaced
production that each location replaces as a producer. With them, from
leakage_summary.csv: what C spares and leaks in all, and C's leakage ratios
(leaked / spared), with the percentiles of each over the draws, which unlike the
cells' are the totals' own. flow_matrices.py turns these into the ratio,
share and net matrices, and documents them.

Written to the run folder's processed/flow_matrices/, as {item_code}_{item}.npz,
with items.csv listing them. Each item is checked against the summary: the rows of
its ratio matrices must sum to C's leakage ratios, those of its spared and leaked
matrices to what C spares and leaks, and those of its replaced shares to C's
replaced share.
"""

import argparse
import contextlib
import multiprocessing
import re
import signal
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

import _run_paths
import _leakage_funcs  # the repository's, on the path from _run_paths
import flow_matrices
from flow_matrices import EXTENSIFIED, METRICS

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("config", nargs="?", type=Path, default=_run_paths.DEFAULT_CONFIG,
                    help="the run's config.json, or its record (default: the repository's config.json)")
parser.add_argument("--workers", type=int, default=1,
                    help="processes to read the files and build the items in at once (default: 1)")
args = parser.parse_args()
if args.workers < 1:
    parser.error(f"--workers must be at least 1, not {args.workers}")

config, RAW_DIR, PROCESSED_DIR, DATA_DIR = _run_paths.run_paths(args.config)
OUT_DIR = PROCESSED_DIR / flow_matrices.SUBDIR
QS: list[float] = config["monte_carlo"]["percentiles"] if config["monte_carlo"]["draws"] else []
PERCENTILES = [f"p{q:g}" for q in QS]
# what's spared is minus a displaced row's value, so its percentile q is minus the row's percentile 100 - q, where the
# run has that one
COMPLEMENT = {f"p{q:g}": f"p{100 - q:g}" for q in QS if 100 - q in QS}
TOLERANCE = 1e-6  # the largest relative difference from the summary the checks allow

SPREAD_COLS = [f"{metric}_{p}" for metric in EXTENSIFIED for p in PERCENTILES]
ROW_COLS = ["COUNTRY_OF_INTEREST_CODE", "flow", "Location_Country_Code", "ItemT_Code", "size", "displaced_kg",
            "production_change_kg", *METRICS, *SPREAD_COLS, "extensification", "fallback_intensity",
            "fallback_extensification", "production_kg"]
RATIO_COLS = [f"{metric}_ratio{suffix}" for metric in EXTENSIFIED for suffix in ["", *(f"_{p}" for p in PERCENTILES)]]
TOTAL_COLS = [f"{metric}_{kind}{suffix}" for metric in METRICS for kind in ["spared", "leaked"]
              for suffix in ["", *(f"_{p}" for p in PERCENTILES if metric in EXTENSIFIED)]]
SUMMARY_COLS = ["COUNTRY_OF_INTEREST_CODE", "ItemT_Code", "ItemT_Name", "size", "displaced_kg", "production_kg",
                "land_use_current_m2", "replaced_share", *TOTAL_COLS, *RATIO_COLS]

isos = _leakage_funcs.load_country_isos(DATA_DIR)
summary = pd.read_csv(RAW_DIR / "leakage_summary.csv", usecols=SUMMARY_COLS)
SIZES = np.unique(summary["size"])  # the same values as the leakage files', as they're written the same way
files = sorted(RAW_DIR.glob("*_leakage.csv"))
print(f"{RAW_DIR}: {len(files)} countries, {summary['ItemT_Code'].nunique()} items and {len(SIZES)} sizes")


def positions(keys: np.ndarray, values: pd.Series, what: str) -> np.ndarray:
    """The position of each of `values` in the sorted `keys`, which must hold them all."""
    values = values.to_numpy()
    i = np.minimum(np.searchsorted(keys, values), len(keys) - 1)
    if (keys[i] != values).any():
        raise ValueError(f"{what} {np.unique(values[keys[i] != values])[:5]} not in the summary")
    return i


def labels(codes: np.ndarray) -> np.ndarray:
    """ISO3 codes for FAOSTAT country codes, or the code where there's none."""
    return np.array([isos.get(code, str(code)) for code in codes])


def read_flows(path: Path) -> pd.DataFrame:
    """One country of interest's rows, from its {ISO}_leakage.csv, with the kg
    produced and the impacts per kg displaced, positive for what's spared in its
    displaced rows. Runs in the workers."""
    rows = pd.read_csv(path, usecols=ROW_COLS)
    rows["replacement"] = rows.pop("flow") == "replacement"
    displaced_kg = rows.pop("displaced_kg")
    rows["production_change_kg"] /= displaced_kg
    sign = np.where(rows["replacement"], 1.0, -1.0)
    rows[METRICS] = rows[METRICS].mul(sign / displaced_kg, axis=0)
    replacement = rows["replacement"].to_numpy()[:, None]
    for metric in EXTENSIFIED:
        cols = [f"{metric}_{p}" for p in PERCENTILES]
        complement = [f"{metric}_{COMPLEMENT[p]}" if p in COMPLEMENT else None for p in PERCENTILES]
        flipped = np.column_stack([-rows[c].to_numpy() if c else np.full(len(rows), np.nan) for c in complement])
        values = np.where(replacement, rows[cols].to_numpy(), flipped) if cols else np.empty((len(rows), 0))
        rows[cols] = (values / displaced_kg.to_numpy()[:, None]).astype(np.float32)
    return rows


def relative_error(row_sums: np.ndarray, expected: np.ndarray, scale: np.ndarray) -> float:
    """The largest difference between `row_sums` and `expected`, relative to
    `scale`, where both are finite."""
    finite = np.isfinite(row_sums) & np.isfinite(expected) & (scale > 0)
    return float(np.max(np.abs(row_sums - expected)[finite] / scale[finite], initial=0))


def build_item(task: tuple[int, pd.DataFrame, pd.DataFrame]) -> dict:
    """Write one item's flow matrices from its leakage rows (`flows`) and its
    summary rows, and return its index entry and checks. Runs in the workers."""
    item_code, flows, rows = task
    countries = np.unique(rows["COUNTRY_OF_INTEREST_CODE"])
    locations = np.unique(flows["Location_Country_Code"])
    shape = (len(SIZES), len(countries), len(locations))
    at_s = positions(SIZES, rows["size"], "sizes"), positions(countries, rows["COUNTRY_OF_INTEREST_CODE"], "countries")
    at_f = (positions(SIZES, flows["size"], "sizes"), positions(countries, flows["COUNTRY_OF_INTEREST_CODE"], "countries"),
            positions(locations, flows["Location_Country_Code"], "locations"))
    replacement = flows["replacement"].to_numpy()

    def grid(column: str) -> np.ndarray:  # sizes x countries, from the summary
        out = np.full(shape[:2], np.nan)
        out[at_s] = rows[column].to_numpy()
        return out

    def cube(column: str, of_replacement: bool) -> np.ndarray:  # sizes x countries x locations, 0 where none
        out = np.zeros(shape)
        keep = replacement == of_replacement
        out[tuple(i[keep] for i in at_f)] = flows[column].to_numpy()[keep]
        return out

    item_name = str(rows["ItemT_Name"].iloc[0])
    country = rows.drop_duplicates("COUNTRY_OF_INTEREST_CODE").set_index("COUNTRY_OF_INTEREST_CODE").loc[countries]
    location = flows.drop_duplicates("Location_Country_Code").set_index("Location_Country_Code").loc[locations]
    replaced = cube("production_change_kg", True)
    arrays = {
        "item_code": np.array(item_code), "item_name": np.array(item_name),
        "run_name": np.array(config["run"]["run_name"]), "size_unit": np.array(config["displacement"]["unit"]),
        "mc_draws": np.array(config["monte_carlo"]["draws"]),
        "sizes": SIZES, "countries": countries, "country_isos": labels(countries),
        "locations": locations, "location_isos": labels(locations),
        "displaced_kg": grid("displaced_kg"),
        "country_production_kg": country["production_kg"].to_numpy(),
        "country_land_use_current_m2": country["land_use_current_m2"].to_numpy(),
        "location_production_kg": location["production_kg"].to_numpy(),
        "location_extensification": location["extensification"].to_numpy(),
        "location_fallback_intensity": location["fallback_intensity"].to_numpy(bool),
        "location_fallback_extensification": location["fallback_extensification"].to_numpy(bool),
        "replaced_share": replaced[0].astype(np.float32),
    }
    checks = {
        "replaced_share_by_size": relative_error(replaced, replaced[:1], np.ones(shape)),
        "replaced_share_total": relative_error(replaced[0].sum(axis=-1), grid("replaced_share")[0],
                                               np.ones(len(countries))),
    }
    for metric in METRICS:
        leaked, spared = cube(metric, True), cube(metric, False)
        spared_total = grid(f"{metric}_spared") / arrays["displaced_kg"]
        leaked_total = grid(f"{metric}_leaked") / arrays["displaced_kg"]
        arrays[f"{metric}_leaked"] = leaked.astype(np.float32)
        arrays[f"{metric}_spared"] = spared.astype(np.float32)
        arrays[f"{metric}_spared_total"] = spared_total
        arrays[f"{metric}_leaked_total"] = leaked_total
        if metric not in EXTENSIFIED:
            continue
        for p in PERCENTILES:
            arrays[f"{metric}_leaked_{p}"] = cube(f"{metric}_{p}", True).astype(np.float32)
            if p in COMPLEMENT:
                arrays[f"{metric}_spared_{p}"] = cube(f"{metric}_{p}", False).astype(np.float32)
            for kind in ["spared", "leaked"]:
                arrays[f"{metric}_{kind}_total_{p}"] = grid(f"{metric}_{kind}_{p}") / arrays["displaced_kg"]
        for column in RATIO_COLS:
            if column.startswith(f"{metric}_ratio"):
                arrays[column] = grid(column)
        # the rows of the spared and leaked matrices sum to what C spares and leaks, and of the ratio matrix to C's
        # leakage ratio, relative to the sizes of their cells
        with np.errstate(divide="ignore", invalid="ignore"):
            checks[f"{metric}_spared"] = relative_error(np.nansum(spared, axis=-1), spared_total,
                                                        np.nansum(np.abs(spared), axis=-1))
            checks[f"{metric}_leaked"] = relative_error(np.nansum(leaked, axis=-1), leaked_total,
                                                        np.nansum(np.abs(leaked), axis=-1))
            checks[f"{metric}_ratio"] = relative_error(np.nansum(leaked, axis=-1) / spared_total,
                                                       arrays[f"{metric}_ratio"],
                                                       np.nansum(np.abs(leaked), axis=-1) / np.abs(spared_total))
    slug = re.sub(r"[^a-z0-9]+", "_", item_name.lower()).strip("_")
    file = f"{item_code}_{slug}.npz"
    np.savez_compressed(OUT_DIR / file, **arrays)
    return {"item_code": item_code, "item_name": item_name, "file": file, "n_countries": len(countries),
            "n_locations": len(locations), **checks}


OUT_DIR.mkdir(parents=True, exist_ok=True)
# the workers are forked, so they share everything loaded above; they ignore Ctrl-C, which stops the run from here
with (multiprocessing.get_context("fork").Pool(args.workers, initializer=signal.signal,
                                               initargs=(signal.SIGINT, signal.SIG_IGN))
      if args.workers > 1 else contextlib.nullcontext()) as pool:
    imap = map if pool is None else pool.imap
    flows = pd.concat(tqdm(imap(read_flows, files), total=len(files), desc="Reading leakage files"),
                      ignore_index=True)
    missing = set(summary["ItemT_Code"]) ^ set(flows["ItemT_Code"])
    if missing:
        raise ValueError(f"Items in only one of the summary and the leakage files: {sorted(missing)}")
    summaries = dict(tuple(summary.groupby("ItemT_Code")))
    tasks = ((item_code, item_flows, summaries[item_code]) for item_code, item_flows in flows.groupby("ItemT_Code"))
    index = pd.DataFrame(list(tqdm(imap(build_item, tasks), total=len(summaries), desc="Building items")))

checks = [col for col in index.columns if col not in ["item_code", "item_name", "file", "n_countries", "n_locations"]]
index.drop(columns=checks).to_csv(OUT_DIR / flow_matrices.INDEX, index=False)
print("Largest relative differences from the summary: "
      + ", ".join(f"{check} {index[check].max():.1e}" for check in checks))
failed = index.loc[(index[checks] > TOLERANCE).any(axis=1), ["item_code", "item_name", *checks]]
if len(failed):
    print(f"Warning: {len(failed)} items differ from the summary by more than {TOLERANCE}:\n{failed.to_string()}")
print(f"Wrote the flow matrices of {len(index)} items to {OUT_DIR}")
