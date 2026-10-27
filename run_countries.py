# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 12:33:50 2025

@author: Tom B

Market leakage of displaced production for every country x item, with the settings
in a config.json (see the README): `python run_countries.py [config.json] [--workers N]`,
with N processes running the items in parallel (the results are the same). Inputs:
    FLMP_RESULTS_DIR  a FLMP results dir, for {year}/.mrio/TradeMatrix_import_dry_matter.csv
                      (only read from)
    IMPACTS_PATH      per-kg production impacts and production, and
    FEED_MIX_PATH     the feed by crop and origin per kg of each animal product, both
                      built from FLMP_RESULTS_DIR into IMPACTS_DIR by
                      _build_bd_prod_impacts_matrix.py if missing
    DATA_DIR          country_codes.csv and commodity_crosswalk.csv, plus beta_crops.csv
                      and beta_animals.csv from _get_extensification_data.py (or the
                      BETA_FILES), for the betas (beta_{year}, se_{year}) of FLMP's
                      year (or BETA_YEAR)
For each displacement size, the results go to RESULTS_DIR/RUN_NAME/raw_outputs, with
the run's record (its config plus the repository version and input checksums) as
RESULTS_DIR/RUN_NAME/config.json, from which it can be rerun.
"""

import argparse
import contextlib
import multiprocessing
import os
import signal
from pathlib import Path

import _run_config

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("config", nargs="?", type=Path, default=_run_config.REPO_DIR / "config.json",
                    help="a config.json, or a run's record of one (default: the repository's config.json)")
parser.add_argument("--workers", type=int, default=1,
                    help="processes to compute the items and write the results in at once (default: 1); see the "
                         "README's Running in parallel")
args = parser.parse_args()
if args.workers < 1:
    parser.error(f"--workers must be at least 1, not {args.workers}")
WORKERS: int = args.workers
if WORKERS > 1:
    # one BLAS thread per worker, as numpy's OpenBLAS otherwise starts up to 64 in each; this must be set before
    # numpy is imported, and the workers inherit it
    for var in ["OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS"]:
        os.environ.setdefault(var, "1")

import numpy as np
import pandas as pd
from tqdm import tqdm

import _build_bd_prod_impacts_matrix
import _leakage_funcs

config = _run_config.load(args.config)

### Configuration variables, from the config (relative paths from the repository folder) ###
RUN_NAME: str = config["run"]["run_name"]
YEAR: int = config["run"]["year"]  # of the FLMP results, and of the betas unless BETA_YEAR
COUNTRIES: list[str] | None = config["run"]["countries"]  # ISO3 codes, e.g. ["GBR"]; None runs every country

FLMP_RESULTS_DIR = _run_config.resolve(config["paths"]["flmp_results_dir"])
RESULTS_DIR = _run_config.resolve(config["paths"]["results_dir"])
RUN_DIR = RESULTS_DIR / RUN_NAME
OUT_DIR = RUN_DIR / _run_config.RAW_OUTPUTS  # the results files
# where _build_bd_prod_impacts_matrix writes its outputs; None for RUN_DIR/inputs/impacts_matrices
IMPACTS_DIR = (_run_config.resolve(config["paths"]["impacts_dir"])
               or RUN_DIR / _run_config.INPUTS / "impacts_matrices")
IMPACTS_PATH = IMPACTS_DIR / f"prod_impacts_long_{YEAR}.csv"
FEED_MIX_PATH = IMPACTS_DIR / f"feed_mix_long_{YEAR}.csv"
DATA_DIR = _run_config.resolve(config["paths"]["data_dir"])

# kg displaced in each country of interest, or a share of its FLMP production of the item
SIZE_UNIT: str = config["displacement"]["unit"]
SIZES: list[float] = config["displacement"]["sizes"]

if len(SIZES) == 0:
    SIZES_DIST: str = config["displacement"]["sizes_dist"]
    SIZES_N: int = config["displacement"]["sizes_n"]
    if SIZES_DIST == "geometric":
        SIZES = np.geomspace(config["displacement"]["sizes_range"][0], config["displacement"]["sizes_range"][1], SIZES_N).tolist()
    elif SIZES_DIST == "arithmetic":
        SIZES = np.linspace(config["displacement"]["sizes_range"][0], config["displacement"]["sizes_range"][1], SIZES_N).tolist()
    SIZES = sorted(set(SIZES))

COMPREHENSIVE: bool = config["replacement"]["comprehensive"]
EXCLUDE_SELF: bool = config["replacement"]["exclude_self"]

USE_BETA: bool = config["extensification"]["use_beta"]  # the betas in DATA_DIR, or else VALUE and STD for every one
# the year of the betas' beta_{year} and se_{year} columns; None for YEAR
BETA_YEAR: int = YEAR if config["extensification"]["beta_year"] is None else config["extensification"]["beta_year"]
BETA_FILES: dict[str, str] = config["extensification"]["beta_files"]  # the "crops" and "animals" betas, in DATA_DIR
EXTENSIFICATION_VALUE: float | None = config["extensification"]["value"]
EXTENSIFICATION_STD: float | None = config["extensification"]["std"]
CLIPPING = None if config["extensification"]["clipping"] is None else tuple(config["extensification"]["clipping"])

DRAWS: int = config["monte_carlo"]["draws"]
SEED: int = config["monte_carlo"]["seed"]
PERCENTILES: list[float] = config["monte_carlo"]["percentiles"]

PER_KG: bool = config["outputs"]["per_kg"]
PER_M2: bool = config["outputs"]["per_m2"]

RUN_DIR.mkdir(exist_ok=True, parents=True)
_run_config.check_run_dir(config, RUN_DIR)
OUT_DIR.mkdir(exist_ok=True)

if not (IMPACTS_PATH.exists() and FEED_MIX_PATH.exists()):
    print(f"{IMPACTS_PATH} or {FEED_MIX_PATH} not found, building them from {FLMP_RESULTS_DIR}")
    _build_bd_prod_impacts_matrix.main(FLMP_RESULTS_DIR, YEAR, DATA_DIR, IMPACTS_DIR)

isos = _leakage_funcs.load_country_isos(DATA_DIR)
item_names = _leakage_funcs.load_item_names(DATA_DIR)
codes, matrices = _leakage_funcs.load_trade_matrices(FLMP_RESULTS_DIR, YEAR)
intensities, production, impact_item_names = _leakage_funcs.load_intensities(IMPACTS_PATH, codes)
for item_code in production:  # where prod_impacts_long has none, the trade matrix's production
    if item_code in matrices:
        traded = matrices[item_code].sum(axis=0) * 1000
        production[item_code] = np.where(production[item_code] > 0, production[item_code],
                                         np.where(traded > 0, traded, np.nan))
items = sorted(set(matrices) & set(intensities))
feed_mix = _leakage_funcs.load_feed_mix(FEED_MIX_PATH, codes)
feed_items = sorted({feed_item for mix in feed_mix.values() for feed_item in mix["feed_item"]})
pasture_based = _leakage_funcs.pasture_based_production(intensities, production, len(codes))
beta_hat, beta_se, estimates = _leakage_funcs.load_extensification(
    USE_BETA, EXTENSIFICATION_VALUE, EXTENSIFICATION_STD, DATA_DIR, BETA_FILES, BETA_YEAR, codes,
    sorted(set(items) | set(feed_items)), intensities)
betas = _leakage_funcs.draw_betas(beta_hat, beta_se, CLIPPING, DRAWS, SEED)

if COUNTRIES is None:
    coi_idx = np.array([i for i, code in enumerate(codes) if code in isos])
else:
    code_of = {iso: code for code, iso in isos.items()}
    missing = [iso for iso in COUNTRIES if code_of.get(iso) not in codes]
    if missing:
        raise ValueError(f"Not in the FLMP trade matrix: {missing}")
    coi_idx = np.searchsorted(codes, [code_of[iso] for iso in COUNTRIES])

no_impacts = sorted(set(matrices) - set(intensities))
if no_impacts:
    print(f"Skipping {len(no_impacts)} traded items with no FLMP impacts: {no_impacts}")
no_betas = [item for item in sorted(set(items) | set(feed_items))
            if item not in _leakage_funcs.PASTURE_ITEMS and (estimates[item][0] < 0).all()]
if no_betas:
    print(f"No betas for {len(no_betas)} items, so no extensified results for them, and their feed left out of "
          f"animal products': {no_betas}")


def label(table: pd.DataFrame, item_code: int, size: float) -> pd.DataFrame:
    """Add the item, the size and the countries' codes and ISO3s to a table from
    leakage_for_item, in place of its coi_idx and location_idx."""
    out = pd.DataFrame({
        "COUNTRY_OF_INTEREST": [isos.get(code) for code in codes[table["coi_idx"]]],
        "COUNTRY_OF_INTEREST_CODE": codes[table["coi_idx"]],
    })
    if "location_idx" in table:
        out["flow"] = table["flow"].to_numpy()
        out["Location_ISO"] = [isos.get(code) for code in codes[table["location_idx"]]]
        out["Location_Country_Code"] = codes[table["location_idx"]]
    out["ItemT_Name"] = item_names.get(item_code, impact_item_names.get(item_code))
    out["ItemT_Code"] = item_code
    out["size"] = size
    rest = table.drop(columns=[col for col in ["coi_idx", "location_idx", "flow"] if col in table])
    return pd.concat([out, rest.reset_index(drop=True)], axis=1)


written = set()  # the files this run has written, which later sizes append to


def write_csv(task: tuple[pd.DataFrame, Path, bool]) -> None:
    """Write `table` to `path` for task (table, path, append): appending to it, or
    replacing any earlier file. Runs in the workers."""
    table, path, append = task
    table.to_csv(path, mode="a" if append else "w", header=not append, index=False)


def item_leakage(task: tuple[float, int]) -> tuple[pd.DataFrame, pd.DataFrame | None, pd.DataFrame] | None:
    """The labelled main, per-kg (None if no output needs it) and summary tables
    of displacing `size` of the item in each country of interest that produces
    it, for task (size, item_code), or None if none does. Runs in the workers."""
    size, item_code = task
    R = matrices[item_code]
    producing_idx = coi_idx[R[:, coi_idx].sum(axis=0) > 0]
    if len(producing_idx) == 0:
        return None
    L = _leakage_funcs.replacement_shares(R, comprehensive=COMPREHENSIVE, exclude_self=EXCLUDE_SELF)
    intensity, fallback = intensities[item_code]
    sources = _leakage_funcs.land_sources(item_code, intensities, production, pasture_based, estimates, feed_mix)
    displaced_kg = (np.full(len(producing_idx), float(size)) if SIZE_UNIT == "kg"
                    else size * production[item_code][producing_idx])
    main, per_kg, summary = _leakage_funcs.leakage_for_item(
        L, intensity, fallback, producing_idx, sources, displaced_kg, estimates[item_code], betas, PERCENTILES,
        production[item_code], pasture_based if item_code in _leakage_funcs.PASTURE_ITEMS else None)
    return (label(main, item_code, size), label(per_kg, item_code, size) if PER_KG or PER_M2 else None,
            label(summary, item_code, size))


# the workers are forked, so they share everything loaded above; they ignore Ctrl-C, which stops the run from here
with (multiprocessing.get_context("fork").Pool(WORKERS, initializer=signal.signal,
                                               initargs=(signal.SIGINT, signal.SIG_IGN))
      if WORKERS > 1 else contextlib.nullcontext()) as pool:
    # imap keeps the items in order, so the files are the same as a serial run's; with one worker, the builtin map
    # runs everything here instead
    imap = map if pool is None else pool.imap
    results = imap(item_leakage, [(SIZES[0], item_code) for item_code in items])
    for i, size in enumerate(SIZES):
        current = results
        if i + 1 < len(SIZES):  # queue the next size behind this one, so the workers go straight on to it
            results = imap(item_leakage, [(SIZES[i + 1], item_code) for item_code in items])
        item_tables = [t for t in tqdm(current, total=len(items), desc=f"Displacing {size} {SIZE_UNIT}")
                       if t is not None]
        mains, per_kgs, summaries = zip(*item_tables)

        main = pd.concat(mains, ignore_index=True)
        summary = pd.concat(summaries, ignore_index=True)
        unextensified = summary["land_use_m2_spared"].isna() & ~summary["ItemT_Code"].isin(no_betas)
        if unextensified.any():
            print(f"No extensified results for {unextensified.sum()} country x items displacing {size} {SIZE_UNIT}, "
                  f"where a land source would lose more than it produces, or with no data for any of their land")
        files = {OUT_DIR / f"{iso}_leakage.csv": country for iso, country in main.groupby("COUNTRY_OF_INTEREST")}
        files[OUT_DIR / "leakage_summary.csv"] = summary
        if PER_KG or PER_M2:
            per_kg = pd.concat(per_kgs, ignore_index=True)
            if PER_KG:
                files.update({OUT_DIR / f"{iso}_leakage_per_kg.csv": country
                              for iso, country in per_kg.groupby("COUNTRY_OF_INTEREST")})
            if PER_M2:
                per_m2 = _leakage_funcs.per_m2_of_spared_land(per_kg)
                files.update({OUT_DIR / f"{iso}_leakage_per_m2.csv": country
                              for iso, country in per_m2.groupby("COUNTRY_OF_INTEREST")})
        # each file gets one table a size, so the workers can write them all at once; waiting for them keeps each
        # file's sizes in order
        list(imap(write_csv, [(table, path, path in written) for path, table in files.items()]))
        written.update(files)

inputs = [FLMP_RESULTS_DIR / str(YEAR) / ".mrio" / "TradeMatrix_import_dry_matter.csv", IMPACTS_PATH, FEED_MIX_PATH]
inputs += [DATA_DIR / name for name in ["country_codes.csv", "commodity_crosswalk.csv", BETA_FILES["crops"],
                                        BETA_FILES["animals"]]]
record_path = _run_config.write_record(config, args.config, RUN_DIR, inputs)

print(f"Wrote leakage for {main['COUNTRY_OF_INTEREST'].nunique()} countries and {len(SIZES)} displacement sizes to "
      f"{OUT_DIR}, and the run's record to {record_path}")
