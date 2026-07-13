# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 10:53:22 2024

@author: Thomas Ball
"""
import os
from functools import lru_cache
import numpy as np
import pandas as pd

import math


@lru_cache(maxsize=None)
def _load_impacts_full(path: str) -> pd.DataFrame:
    """Cached read of an impacts_full.csv - avoids re-reading/re-parsing the
    same file for every (country, item_code) call that touches it."""
    return pd.read_csv(path, index_col=0)


def round_sig_vec(x: pd.Series, sig: int = 6) -> pd.Series:
    """Vectorized equivalent of applying round_sig() to every element of x."""
    arr = np.asarray(x, dtype=float)
    out = np.zeros_like(arr)
    nz = arr != 0
    with np.errstate(divide="ignore", invalid="ignore"):
        magnitude = np.floor(np.log10(np.abs(arr[nz])))
        factor = 10.0 ** (sig - 1 - magnitude)
        out[nz] = np.round(arr[nz] * factor) / factor
    return pd.Series(out, index=x.index)

def read_coi_exports(rpath: str, 
                     coi_iso: str, 
                     coi_code: int, 
                     comm_of_interest_code=None,
                     COMPREHENSIVE: bool = True, # run for exports too or just coi
                     SELF_EXCL = False, 
                     QUIET: bool = True) -> pd.DataFrame:
    """Walks a given path and builds a dataframe of the exports of the given item from
    the given country using csv files from each folder in the path
    """
    coi_exports: pd.DataFrame = pd.DataFrame()

    if COMPREHENSIVE: 
        paths = os.listdir(rpath)
    elif not COMPREHENSIVE:
        paths = [coi_iso]
    else:
        raise ValueError("Invalid value for COMPREHENSIVE parameter")
    
    frames: list[pd.DataFrame] = []
    for path in paths:
        if len(path) != 3:
            continue
        csv_path = os.path.join(rpath, path, "impacts_full.csv")
        if os.path.exists(csv_path):

            impacts_full: pd.DataFrame = _load_impacts_full(csv_path)

            impacts_filtered: pd.DataFrame = impacts_full[
                                    impacts_full["ItemT_Code"] == comm_of_interest_code
                                    ].copy()

            impacts_self: pd.DataFrame = impacts_filtered[impacts_filtered["Producer_Country_Code"] == coi_code]

            if len(impacts_self) > 0:

                impacts_filtered["export_val"] = impacts_filtered.loc[
                    impacts_filtered["Producer_Country_Code"] == coi_code, "provenance"
                ].sum()

                if SELF_EXCL and not COMPREHENSIVE:
                    impacts_filtered = impacts_filtered[impacts_filtered["Producer_Country_Code"] != coi_code]
                    impacts_filtered["export_val"] = 1

                import_ratio = (
                    impacts_filtered.groupby("Country_ISO")["provenance"].sum()
                    / impacts_filtered["provenance"].sum()
                )

                impacts_filtered["import_ratio"] = impacts_filtered["Country_ISO"].map(import_ratio)

                impacts_filtered["Consumer_ISO3"] = path.upper()
                frames.append(impacts_filtered)
        else:
            if not QUIET:
                print(f"Couldn't find impacts_full.csv for {path} at {csv_path}")

    if frames:
        coi_exports = pd.concat(frames)

    if not coi_exports.empty:

        coi_exports["export_weight"] = (
            coi_exports["export_val"] / coi_exports["export_val"].unique().sum()
        )

    return coi_exports

def round_sig(x, sig=6):
    if x == 0:
        return 0
    return round(x, sig - int(math.floor(math.log10(abs(x)))) - 1)

def return_leakage_df(  COUNTRY_OF_INTEREST, 
                        ITEM_OF_INTEREST : str, 
                        RPATH, 
                        item_code, countries, production_data,
                        COMPREHENSIVE = True, # run for exports too or just coi
                        NO_ANIM=False,
                        SELF_EXCL=False,
                        QUIET=True):
    
    if COUNTRY_OF_INTEREST not in countries:
        print("Country not in set of countries")
        raise ValueError
    
    production_data = production_data[production_data["Year"] == production_data["Year"].unique().max()]

    # # calculate some leakage
    coi_impacts_data = _load_impacts_full(os.path.join(RPATH, COUNTRY_OF_INTEREST, "impacts_full.csv"))
    coi_impacts_item = coi_impacts_data[coi_impacts_data.ItemT_Code == item_code]
    is_anim = coi_impacts_item.Animal_Product.str.contains("Primary").any()

    if is_anim and NO_ANIM:
        if not QUIET:
            print(f"Skipping {ITEM_OF_INTEREST} for {COUNTRY_OF_INTEREST} because it's an animal product and NO_ANIM is set to True")
        return None
    
    _internal = coi_impacts_item[coi_impacts_item["Producer_Country_Code"] == countries[COUNTRY_OF_INTEREST]]
    
    internal_bd_pm2 = _internal['bd_opp_cost_m2']

    internal_prov_tonnes = _internal['provenance']
    internal_land_use_pasture_m2_pkg = _internal['Pasture_m2'] / (1000 * internal_prov_tonnes)
    internal_land_use_arable_m2_pkg = _internal['Arable_m2'] / (1000 * internal_prov_tonnes)
    
    internal_yield_arable_kg_m2 = 1 / internal_land_use_arable_m2_pkg[internal_land_use_arable_m2_pkg > 0]
    internal_yield_pasture_kg_m2 = 1 / internal_land_use_pasture_m2_pkg[internal_land_use_pasture_m2_pkg > 0]

    coi_exports = read_coi_exports( RPATH, COUNTRY_OF_INTEREST,
                                    countries[COUNTRY_OF_INTEREST],
                                    COMPREHENSIVE=COMPREHENSIVE, 
                                    comm_of_interest_code=item_code,
                                    QUIET=QUIET,
                                    SELF_EXCL=SELF_EXCL)

    if len(coi_exports) == 0:   
        if not QUIET:
            print(
                f"Couldn't find any exports of {ITEM_OF_INTEREST} for {COUNTRY_OF_INTEREST}"
        )
        return None
    
    if is_anim:
        # this is for monogastric (i.e. no pasture land use) - assume land use leakage is entirely feed
        if isinstance(internal_yield_pasture_kg_m2, pd.Series) and len(internal_yield_pasture_kg_m2) == 0:
            displaced_production_mass_kg = 1.0
        else:
            displaced_production_mass_kg = float(internal_yield_pasture_kg_m2.squeeze())
    else:
        if isinstance(internal_yield_arable_kg_m2, pd.Series) and len(internal_yield_arable_kg_m2) == 0:
            displaced_production_mass_kg = 1.0
        else:
            displaced_production_mass_kg = float(internal_yield_arable_kg_m2.squeeze())
    
    coi_exports = coi_exports.reset_index()

    coi_exports["idisplaced_production_kg"] = displaced_production_mass_kg \
                * coi_exports["export_weight"] \
                * (coi_exports["import_ratio"] #/ np.nansum(coi_exports["import_ratio"])
                   )
    
    coi_exports["pasture_m2_per_kg"] = round_sig_vec(coi_exports['Pasture_m2'] / (1000 * coi_exports['provenance']))
    coi_exports["arable_m2_per_kg"] = round_sig_vec(coi_exports['Arable_m2'] / (1000 * coi_exports['provenance']))
    coi_exports["land_use_total_m2_per_kg"] = round_sig_vec(coi_exports["arable_m2_per_kg"] + coi_exports["pasture_m2_per_kg"])

    coi_exports["bd_per_m2"] = coi_exports['bd_opp_cost_m2']
    coi_exports["bd_per_m2_err"] = coi_exports['opp_cost_err'] / 1E6

    coi_exports["swwu_l_per_kg"] = round_sig_vec((coi_exports['SWWU_avg_calc'] / (1000 * coi_exports['provenance'])).fillna(0))

    coi_exports["ghg_kg_co2eq_per_kg"] = round_sig_vec((coi_exports['GHG_avg_calc'] / (1000 * coi_exports['provenance'])).fillna(0))

    incl = ["Consumer_Country_Code", "Consumer_ISO3", # consumer
            "Producer_Country_Code", "Country_ISO", # producer
            "ItemT_Name", "ItemT_Code",
            "Item", "Item_Code",
            "pasture_m2_per_kg", 
            "arable_m2_per_kg", 
            "land_use_total_m2_per_kg",
            "bd_per_m2", "bd_per_m2_err",
            "swwu_l_per_kg",
            "ghg_kg_co2eq_per_kg", 
            "idisplaced_production_kg"]
    

    coi_exports = coi_exports[incl]
    coi_exports.rename(columns={"Consumer_ISO3": "Consumer_ISO",
                                              "Country_ISO": "Producer_ISO"}, 
                                              inplace=True)

    coi_exports["pasture_m2_leakage"] = coi_exports["pasture_m2_per_kg"] * coi_exports["idisplaced_production_kg"]
    coi_exports["arable_m2_leakage"] = coi_exports["arable_m2_per_kg"] * coi_exports["idisplaced_production_kg"]
    coi_exports["land_use_total_m2_leakage"] = coi_exports["land_use_total_m2_per_kg"] * coi_exports["idisplaced_production_kg"]
    coi_exports["bd_leakage"] = coi_exports["bd_per_m2"] * coi_exports["land_use_total_m2_leakage"]
    coi_exports["bd_leakage_err"] = coi_exports["bd_per_m2_err"] * coi_exports["land_use_total_m2_leakage"]
    coi_exports["swwu_l_leakage"] = coi_exports["swwu_l_per_kg"] * coi_exports["idisplaced_production_kg"]   
    coi_exports["ghg_kg_co2eq_leakage"] = coi_exports["ghg_kg_co2eq_per_kg"] * coi_exports["idisplaced_production_kg"]

    keep = ["Producer_ISO", "Producer_Country_Code", 
            "ItemT_Name", "ItemT_Code",
            "Item", "Item_Code",
            "idisplaced_production_kg",
            "pasture_m2_leakage", 
            "arable_m2_leakage", 
            "land_use_total_m2_leakage", 
            "bd_leakage", 
            "bd_leakage_err", 
            "swwu_l_leakage", 
            "ghg_kg_co2eq_leakage"]

    coi_exports = coi_exports[keep]
    
    coi_exports["Producer_ISO"] = coi_exports["Producer_ISO"].astype(str).str.strip()
    coi_exports["Producer_Country_Code"] = coi_exports["Producer_Country_Code"].astype(int)
    coi_exports["ItemT_Name"] = coi_exports["ItemT_Name"].astype(str).str.strip()
    coi_exports["ItemT_Code"] = coi_exports["ItemT_Code"].astype(int)

    leakage_calc = coi_exports[[_ for _ in keep if _ not in ["Item", "Item_Code"]]].groupby(["Producer_ISO", "Producer_Country_Code", "ItemT_Name", "ItemT_Code"]).sum().reset_index()
    displaced_production_full = coi_exports.groupby(["Producer_ISO", "Producer_Country_Code", "ItemT_Name", "ItemT_Code", "Item", "Item_Code"]).sum().reset_index()

    internal_rows = {
                    "Producer_ISO" : COUNTRY_OF_INTEREST,
                    "Producer_Country_Code" : countries[COUNTRY_OF_INTEREST],
                    "ItemT_Name" : ITEM_OF_INTEREST[0],
                    "ItemT_Code" : item_code,
                    "idisplaced_production_kg" : - displaced_production_mass_kg,
                    "pasture_m2_leakage" : - internal_land_use_pasture_m2_pkg.squeeze() * displaced_production_mass_kg,
                    "arable_m2_leakage" : - internal_land_use_arable_m2_pkg.squeeze() * displaced_production_mass_kg,
                    "land_use_total_m2_leakage" : -(internal_land_use_pasture_m2_pkg.squeeze() + internal_land_use_arable_m2_pkg.squeeze()) * displaced_production_mass_kg,
                    "bd_leakage" : - internal_bd_pm2.squeeze(), 
                    "bd_leakage_err" : _internal["opp_cost_err"].squeeze() / 1E6,
                    "swwu_l_leakage" : - _internal["SWWU_avg_calc"].squeeze() * displaced_production_mass_kg /  (1000 * internal_prov_tonnes),
                    "ghg_kg_co2eq_leakage" : - _internal["GHG_avg_calc"].squeeze() * displaced_production_mass_kg /  (1000 * internal_prov_tonnes),
                }
    
    internal_anti_leakage = pd.DataFrame(internal_rows, index=[0])

    leakage_calc = pd.concat([leakage_calc, internal_anti_leakage])
    
    leakage_calc = leakage_calc.loc[:, ~(leakage_calc.columns.str.contains("Unnamed"))]

    leakage_calc_per_kg = leakage_calc.copy()

    pkg_cols = [
            "idisplaced_production_kg",
            "pasture_m2_leakage", 
            "arable_m2_leakage", 
            "land_use_total_m2_leakage", 
            "bd_leakage", 
            "bd_leakage_err", 
            "swwu_l_leakage", 
            "ghg_kg_co2eq_leakage"]

    for col in pkg_cols:
        leakage_calc_per_kg[col] = leakage_calc_per_kg[col] / displaced_production_mass_kg
        leakage_calc_per_kg.rename(columns={col: f"{col}_per_kg"}, inplace=True) 

    return (leakage_calc, leakage_calc_per_kg, displaced_production_full)
