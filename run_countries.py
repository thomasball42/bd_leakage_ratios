# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 12:33:50 2025

@author: Tom B

Market leakage of displaced production for every country x item, with the settings
in a config.json (see the README): `python run_countries.py [config.json]`. Inputs:
    FLMP_RESULTS_DIR  a FLMP results dir, for {year}/.mrio/TradeMatrix_import_dry_matter.csv
                      (only read from)
    IMPACTS_PATH      per-kg production impacts and production, and
    FEED_MIX_PATH     the feed by crop and origin per kg of each animal product, both
                      built from FLMP_RESULTS_DIR into IMPACTS_DIR by
                      _build_bd_prod_impacts_matrix.py if missing
    DATA_DIR          country_codes.csv and commodity_crosswalk.csv, plus beta_crops.csv
                      and beta_animals.csv from _get_extensification_data.py, for the
                      betas (beta_{year}, se_{year}) of FLMP's year
For each displacement size, the results go to RESULTS_DIR/RUN_NAME, with the run's
record (its config plus the repository version and input checksums) as config.json,
from which it can be rerun.
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

import _build_bd_prod_impacts_matrix
import _leakage_funcs
import _run_config

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("config", nargs="?", type=Path, default=_run_config.REPO_DIR / "config.json",
                    help="a config.json, or a run's record of one (default: the repository's config.json)")
args = parser.parse_args()
config = _run_config.load(args.config)

### Configuration variables, from the config (relative paths from the repository folder) ###
RUN_NAME: str = config["run"]["run_name"]
YEAR: int = config["run"]["year"]  # of the FLMP results and of the betas
COUNTRIES: list[str] | None = config["run"]["countries"]  # ISO3 codes, e.g. ["GBR"]; None runs every country

FLMP_RESULTS_DIR = _run_config.resolve(config["paths"]["flmp_results_dir"])
RESULTS_DIR = _run_config.resolve(config["paths"]["results_dir"])
RUN_DIR = RESULTS_DIR / RUN_NAME
# where _build_bd_prod_impacts_matrix writes its outputs; None for RUN_DIR/impacts_matrices
IMPACTS_DIR = _run_config.resolve(config["paths"]["impacts_dir"]) or RUN_DIR / "impacts_matrices"
IMPACTS_PATH = IMPACTS_DIR / f"prod_impacts_long_{YEAR}.csv"
FEED_MIX_PATH = IMPACTS_DIR / f"feed_mix_long_{YEAR}.csv"
DATA_DIR = _run_config.resolve(config["paths"]["data_dir"])

# kg displaced in each country of interest, or a share of its FLMP production of the item
SIZE_UNIT: str = config["displacement"]["unit"]
SIZES: list[float] = config["displacement"]["sizes"]
COMPREHENSIVE: bool = config["replacement"]["comprehensive"]
EXCLUDE_SELF: bool = config["replacement"]["exclude_self"]

USE_BETA: bool = config["extensification"]["use_beta"]  # the betas in DATA_DIR, or else VALUE and STD for every one
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
    USE_BETA, EXTENSIFICATION_VALUE, EXTENSIFICATION_STD, DATA_DIR, YEAR, codes, sorted(set(items) | set(feed_items)),
    intensities)
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
    leakage_for_item, in place of its coi_idx and producer_idx."""
    out = pd.DataFrame({
        "COUNTRY_OF_INTEREST": [isos.get(code) for code in codes[table["coi_idx"]]],
        "COUNTRY_OF_INTEREST_CODE": codes[table["coi_idx"]],
    })
    if "producer_idx" in table:
        out["flow"] = table["flow"].to_numpy()
        out["Producer_ISO"] = [isos.get(code) for code in codes[table["producer_idx"]]]
        out["Producer_Country_Code"] = codes[table["producer_idx"]]
    out["ItemT_Name"] = item_names.get(item_code, impact_item_names.get(item_code))
    out["ItemT_Code"] = item_code
    out["size"] = size
    rest = table.drop(columns=[col for col in ["coi_idx", "producer_idx", "flow"] if col in table])
    return pd.concat([out, rest.reset_index(drop=True)], axis=1)


written = set()


def write(table: pd.DataFrame, path: Path) -> None:
    """Write `table` to `path`, replacing any earlier file the first time this run
    writes it and appending after that."""
    table.to_csv(path, mode="a" if path in written else "w", header=path not in written, index=False)
    written.add(path)


for size in SIZES:
    mains, per_kgs, summaries = [], [], []
    for item_code in tqdm(items, desc=f"Displacing {size} {SIZE_UNIT}"):
        R = matrices[item_code]
        producing_idx = coi_idx[R[:, coi_idx].sum(axis=0) > 0]
        if len(producing_idx) == 0:
            continue
        L = _leakage_funcs.replacement_shares(R, comprehensive=COMPREHENSIVE, exclude_self=EXCLUDE_SELF)
        intensity, fallback = intensities[item_code]
        sources = _leakage_funcs.land_sources(item_code, intensities, production, pasture_based, estimates, feed_mix)
        displaced_kg = (np.full(len(producing_idx), float(size)) if SIZE_UNIT == "kg"
                        else size * production[item_code][producing_idx])
        main, per_kg, summary = _leakage_funcs.leakage_for_item(
            L, intensity, fallback, producing_idx, sources, displaced_kg, estimates[item_code], betas, PERCENTILES,
            production[item_code], pasture_based if item_code in _leakage_funcs.PASTURE_ITEMS else None)
        mains.append(label(main, item_code, size))
        summaries.append(label(summary, item_code, size))
        if PER_KG or PER_M2:
            per_kgs.append(label(per_kg, item_code, size))

    main = pd.concat(mains, ignore_index=True)
    summary = pd.concat(summaries, ignore_index=True)
    unextensified = summary["land_use_m2_spared"].isna() & ~summary["ItemT_Code"].isin(no_betas)
    if unextensified.any():
        print(f"No extensified results for {unextensified.sum()} country x items displacing {size} {SIZE_UNIT}, where "
              f"a land source would lose more than it produces, or with no data for any of their land")
    for iso, country in main.groupby("COUNTRY_OF_INTEREST"):
        write(country, RUN_DIR / f"{iso}_leakage.csv")
    write(summary, RUN_DIR / "leakage_summary.csv")
    if per_kgs:
        per_kg = pd.concat(per_kgs, ignore_index=True)
        if PER_KG:
            for iso, country in per_kg.groupby("COUNTRY_OF_INTEREST"):
                write(country, RUN_DIR / f"{iso}_leakage_per_kg.csv")
        if PER_M2:
            for iso, country in _leakage_funcs.per_m2_of_spared_land(per_kg).groupby("COUNTRY_OF_INTEREST"):
                write(country, RUN_DIR / f"{iso}_leakage_per_m2.csv")

inputs = [FLMP_RESULTS_DIR / str(YEAR) / ".mrio" / "TradeMatrix_import_dry_matter.csv", IMPACTS_PATH, FEED_MIX_PATH]
inputs += [DATA_DIR / name for name in ["country_codes.csv", "commodity_crosswalk.csv", "beta_crops.csv",
                                        "beta_animals.csv"]]
record_path = _run_config.write_record(config, args.config, RUN_DIR, inputs)

print(f"Wrote leakage for {main['COUNTRY_OF_INTEREST'].nunique()} countries and {len(SIZES)} displacement sizes to "
      f"{RUN_DIR}, and the run's record to {record_path}")
