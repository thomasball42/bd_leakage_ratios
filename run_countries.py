# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 12:33:50 2025

@author: Tom B

Market leakage of displaced production for every country x item, with the settings
in a config.json (see the README): `python run_countries.py [config.json]`. Inputs:
    FLMP_RESULTS_DIR  a FLMP results dir, for {year}/.mrio/TradeMatrix_import_dry_matter.csv
                      (only read from)
    IMPACTS_PATH      per-kg production impacts, and
    FEED_MIX_PATH     the feed by crop and origin per kg of each animal product, both
                      built from FLMP_RESULTS_DIR into IMPACTS_DIR by
                      _build_bd_prod_impacts_matrix.py if missing
    DATA_DIR          country_codes.csv and commodity_crosswalk.csv, plus beta_crops.csv
                      and beta_animals.csv from _get_extensification_data.py, for
                      producers' current production and land use (and the betas, for
                      EXTENSIFICATION_VALUE = "beta")
The results go to RESULTS_DIR/RUN_NAME, with the run's record (its config plus the
repository version and input checksums) as config.json, from which it can be rerun.
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
YEAR: int = config["run"]["year"]
COUNTRIES: list[str] | None = config["run"]["countries"]  # ISO3 codes, e.g. ["GBR"]; None runs every country

FLMP_RESULTS_DIR = _run_config.resolve(config["paths"]["flmp_results_dir"])
RESULTS_DIR = _run_config.resolve(config["paths"]["results_dir"])
RUN_DIR = RESULTS_DIR / RUN_NAME
# where _build_bd_prod_impacts_matrix writes its outputs; None for RUN_DIR/impacts_matrices
IMPACTS_DIR = _run_config.resolve(config["paths"]["impacts_dir"]) or RUN_DIR / "impacts_matrices"
IMPACTS_PATH = IMPACTS_DIR / f"prod_impacts_long_{YEAR}.csv"
FEED_MIX_PATH = IMPACTS_DIR / f"feed_mix_long_{YEAR}.csv"
DATA_DIR = _run_config.resolve(config["paths"]["data_dir"])

COMPREHENSIVE: bool = config["replacement"]["comprehensive"]
EXCLUDE_SELF: bool = config["replacement"]["exclude_self"]

EXTENSIFICATION_VALUE: float | str = config["extensification"]["value"]
EXTENSIFICATION_STD: float = config["extensification"]["std"]
CLIPPING = None if config["extensification"]["clipping"] is None else tuple(config["extensification"]["clipping"])

RUN_DIR.mkdir(exist_ok=True, parents=True)
_run_config.check_run_dir(config, RUN_DIR)

if not (IMPACTS_PATH.exists() and FEED_MIX_PATH.exists()):
    print(f"{IMPACTS_PATH} or {FEED_MIX_PATH} not found, building them from {FLMP_RESULTS_DIR}")
    _build_bd_prod_impacts_matrix.main(FLMP_RESULTS_DIR, YEAR, DATA_DIR, IMPACTS_DIR)

isos = _leakage_funcs.load_country_isos(DATA_DIR)
item_names = _leakage_funcs.load_item_names(DATA_DIR)
codes, matrices = _leakage_funcs.load_trade_matrices(FLMP_RESULTS_DIR, YEAR)
intensities, impact_item_names = _leakage_funcs.load_intensities(IMPACTS_PATH, codes)
items = sorted(set(matrices) & set(intensities))
feed_mix = _leakage_funcs.load_feed_mix(FEED_MIX_PATH, codes)
feed_items = sorted({feed_item for mix in feed_mix.values() for feed_item in mix["feed_item"]})
extensification = _leakage_funcs.load_extensification(EXTENSIFICATION_VALUE, EXTENSIFICATION_STD, CLIPPING, DATA_DIR,
                                                      YEAR, codes, sorted(set(items) | set(feed_items)), intensities)

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
no_extensification = [item for item in sorted(set(items) | set(feed_items))
                      if np.isnan(extensification[item][0]).any(axis=1).all()]
if no_extensification:
    print(f"No extensification data for {len(no_extensification)} items, so no {_leakage_funcs.EXT_COLS} for them "
          f"and their feed left out of animal products': {no_extensification}")

tables = []
for item_code in tqdm(items, desc="Processing items"):
    R = matrices[item_code]
    producing_idx = coi_idx[R[:, coi_idx].sum(axis=0) > 0]
    if len(producing_idx) == 0:
        continue
    L = _leakage_funcs.replacement_shares(R, comprehensive=COMPREHENSIVE, exclude_self=EXCLUDE_SELF)
    intensity, fallback = intensities[item_code]
    ext, ext_fallback = extensification[item_code]
    sources = _leakage_funcs.land_sources(item_code, intensities, extensification, feed_mix)
    table = _leakage_funcs.leakage_for_item(L, intensity, fallback, producing_idx, ext, ext_fallback, sources)
    table["ItemT_Code"] = item_code
    table["ItemT_Name"] = item_names.get(item_code, impact_item_names.get(item_code))
    tables.append(table)

leakage = pd.concat(tables, ignore_index=True)
leakage["COUNTRY_OF_INTEREST_CODE"] = codes[leakage["coi_idx"]]
leakage["COUNTRY_OF_INTEREST"] = leakage["COUNTRY_OF_INTEREST_CODE"].map(isos)
leakage["Producer_Country_Code"] = codes[leakage["producer_idx"]]
leakage["Producer_ISO"] = leakage["Producer_Country_Code"].map(isos)
leakage = leakage[
    ["COUNTRY_OF_INTEREST", "COUNTRY_OF_INTEREST_CODE", "flow", "Producer_ISO", "Producer_Country_Code",
     "ItemT_Name", "ItemT_Code", "displaced_production_kg", "extensification", "extensification_std",
     *_leakage_funcs.EXT_COLS, "arable_m2", "pasture_m2", "land_use_total_m2",
     "bd", "bd_err", "ghg_prod_kgco2e", "ghg_prod_kgco2e_err", "ghg_coc_kgco2e", "ghg_coc_kgco2e_err",
     "fallback_intensity", "fallback_extensification"]
]
leakage_per_m2 = _leakage_funcs.per_m2_of_spared_land(leakage)

prefix = (("" if COMPREHENSIVE else "NOEXP_") + ("NOSELF_" if EXCLUDE_SELF else "")
          + f"EXT{EXTENSIFICATION_VALUE}_")
for iso, country_leakage in leakage.groupby("COUNTRY_OF_INTEREST"):
    country_leakage.to_csv(RUN_DIR / f"{iso}_{prefix}leakage_per_kg.csv", index=False)
for iso, country_leakage in leakage_per_m2.groupby("COUNTRY_OF_INTEREST"):
    country_leakage.to_csv(RUN_DIR / f"{iso}_{prefix}leakage.csv", index=False)
_leakage_funcs.summarise(leakage).to_csv(RUN_DIR / f"{prefix}leakage_summary_per_kg.csv", index=False)

inputs = [FLMP_RESULTS_DIR / str(YEAR) / ".mrio" / "TradeMatrix_import_dry_matter.csv", IMPACTS_PATH, FEED_MIX_PATH]
inputs += [DATA_DIR / name for name in ["country_codes.csv", "commodity_crosswalk.csv", "beta_crops.csv",
                                        "beta_animals.csv"]]
record_path = _run_config.write_record(config, args.config, RUN_DIR, inputs)

print(f"Wrote leakage for {leakage['COUNTRY_OF_INTEREST'].nunique()} countries to {RUN_DIR}, "
      f"and the run's record to {record_path}")
