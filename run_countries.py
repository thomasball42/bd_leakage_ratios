# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 12:33:50 2025

@author: Tom B

Market leakage of displaced production for every country x item. Inputs:
    FLMP_RESULTS_DIR  a FLMP results dir, for {year}/.mrio/TradeMatrix_import_dry_matter.csv
    IMPACTS_PATH      per-kg production impacts, built from FLMP_RESULTS_DIR by
                      _build_bd_prod_impacts_matrix.py if missing
    DATA_DIR          country_codes.csv and commodity_crosswalk.csv, plus beta_crops.csv
                      and beta_animals.csv for EXTENSIFICATION_VALUE = "beta"
"""

from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

import _build_bd_prod_impacts_matrix
import _leakage_funcs

### Configuration variables ###
YEAR = 2021
FLMP_RESULTS_DIR: Path = Path("/scratch/tsb42/outputs/flmp_results/flmp_results_261009")  # the FLMP results_dir
RESULTS_DIR = Path("../..") / "outputs" / "leakage_results" / "leakage_results_261009"

EXTENSIFICATION_VALUE: float | str = 0.0  # extensification factor passed to _leakage_funcs.leakage_land_use: a float (~0 to ~1) for every producer and item, or "beta" for the per country x item values in data/beta_*.csv
EXTENSIFICATION_STD = 0.0  # its standard deviation, when EXTENSIFICATION_VALUE is a float (the betas carry their own)


IMPACTS_PATH: Path = FLMP_RESULTS_DIR / "impacts_matrices" / f"prod_impacts_long_{YEAR}.csv"  # where _build_bd_prod_impacts_matrix writes it
DATA_DIR: Path = Path("data")

COUNTRIES: list[str] | None = None  # ISO3 codes, e.g. ["GBR"]; None runs every country

COMPREHENSIVE = True  # whether all consumers of the country's production replace it (True) or just the country itself (False) - valid when crops mainly used domestically (e.g., the UK)
EXCLUDE_SELF = False  # whether the country itself is barred from replacing its own displaced production

RESULTS_DIR.mkdir(exist_ok=True, parents=True)

if not IMPACTS_PATH.exists():
    print(f"{IMPACTS_PATH} not found, building it from {FLMP_RESULTS_DIR}")
    _build_bd_prod_impacts_matrix.main(FLMP_RESULTS_DIR, YEAR, DATA_DIR)

isos = _leakage_funcs.load_country_isos(DATA_DIR)
item_names = _leakage_funcs.load_item_names(DATA_DIR)
codes, matrices = _leakage_funcs.load_trade_matrices(FLMP_RESULTS_DIR, YEAR)
intensities, impact_item_names = _leakage_funcs.load_intensities(IMPACTS_PATH, codes)
items = sorted(set(matrices) & set(intensities))
extensification = _leakage_funcs.load_extensification(EXTENSIFICATION_VALUE, EXTENSIFICATION_STD, DATA_DIR, YEAR,
                                                      codes, items)

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

tables = []
for item_code in tqdm(items, desc="Processing items"):
    R = matrices[item_code]
    producing_idx = coi_idx[R[:, coi_idx].sum(axis=0) > 0]
    if len(producing_idx) == 0:
        continue
    L = _leakage_funcs.replacement_shares(R, comprehensive=COMPREHENSIVE, exclude_self=EXCLUDE_SELF)
    intensity, fallback = intensities[item_code]
    ext, ext_std = extensification[item_code]
    current_production = np.full(len(codes), np.nan)  # TODO: each producer's current production of item_code, on the codes index
    table = _leakage_funcs.leakage_for_item(L, intensity, fallback, producing_idx, ext, ext_std, current_production)
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
     "ItemT_Name", "ItemT_Code", "displaced_production_kg", "extensification", "extensification_std", "land_use_leakage_m2", "arable_m2", "pasture_m2", "land_use_total_m2",
     "bd", "bd_err", "ghg_prod_kgco2e", "ghg_prod_kgco2e_err", "ghg_coc_kgco2e", "ghg_coc_kgco2e_err",
     "fallback_intensity"]
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
