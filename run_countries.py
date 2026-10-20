# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 12:33:50 2025

@author: Tom B

Market leakage of displaced production for every country x item. Inputs:
    FLMP_RESULTS_DIR  a FLMP results dir, for {year}/.mrio/TradeMatrix_import_dry_matter.csv
                      (only read from)
    IMPACTS_PATH      per-kg production impacts, and
    FEED_MIX_PATH     the feed by crop and origin per kg of each animal product, both
                      built from FLMP_RESULTS_DIR into RESULTS_DIR/impacts_matrices by
                      _build_bd_prod_impacts_matrix.py if missing
    DATA_DIR          country_codes.csv and commodity_crosswalk.csv, plus beta_crops.csv
                      and beta_animals.csv from _get_extensification_data.py, for
                      producers' current production and land use (and the betas, for
                      EXTENSIFICATION_VALUE = "beta")
"""

from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

import _build_bd_prod_impacts_matrix
import _leakage_funcs

### Configuration variables ###
YEAR = 2021
FLMP_RESULTS_DIR = Path("/maps/tsb42/flmp/flmp_results/261019")  # the FLMP results_dir
RESULTS_DIR = Path("/maps/tsb42/leakage_flows/bd_leakage_ratios_results/261019")

EXTENSIFICATION_VALUE: float | str = 0.0  # extensification factor passed to _leakage_funcs.leakage_land_use: a float (~0 to ~1) for every producer and item, or "beta" for the per country x item values in data/beta_*.csv
EXTENSIFICATION_STD = 0.0  # its standard deviation, when EXTENSIFICATION_VALUE is a float (the betas carry their own)


IMPACTS_DIR = RESULTS_DIR / "impacts_matrices"  # where _build_bd_prod_impacts_matrix writes its outputs
IMPACTS_PATH: Path = IMPACTS_DIR / f"prod_impacts_long_{YEAR}.csv"
FEED_MIX_PATH: Path = IMPACTS_DIR / f"feed_mix_long_{YEAR}.csv"
DATA_DIR: Path = Path("data")

COUNTRIES: list[str] | None = None  # ISO3 codes, e.g. ["GBR"]; None runs every country

COMPREHENSIVE = True  # whether all consumers of the country's production replace it (True) or just the country itself (False) - valid when crops mainly used domestically (e.g., the UK)
EXCLUDE_SELF = False  # whether the country itself is barred from replacing its own displaced production

RESULTS_DIR.mkdir(exist_ok=True, parents=True)

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
extensification = _leakage_funcs.load_extensification(EXTENSIFICATION_VALUE, EXTENSIFICATION_STD, DATA_DIR, YEAR,
                                                      codes, sorted(set(items) | set(feed_items)), intensities)

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
    country_leakage.to_csv(RESULTS_DIR / f"{iso}_{prefix}leakage_per_kg.csv", index=False)
for iso, country_leakage in leakage_per_m2.groupby("COUNTRY_OF_INTEREST"):
    country_leakage.to_csv(RESULTS_DIR / f"{iso}_{prefix}leakage.csv", index=False)
_leakage_funcs.summarise(leakage).to_csv(RESULTS_DIR / f"{prefix}leakage_summary_per_kg.csv", index=False)

print(f"Wrote leakage for {leakage['COUNTRY_OF_INTEREST'].nunique()} countries to {RESULTS_DIR}")
