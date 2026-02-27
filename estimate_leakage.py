import os
from typing import Any
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
import _funcs_data as fd
import _plot_funcs as pf

def return_leakage_df(COUNTRY_OF_INTEREST, ITEM_OF_INTEREST : str, RPATH, 
                        DATA_PATH,
                        item_code, countries, production_data):
    
    if COUNTRY_OF_INTEREST not in countries:
        print("Country not in set of countries")
        raise ValueError
    
    production_data = production_data[production_data["Year"] == production_data["Year"].unique().max()]
    yield_data: pd.DataFrame = production_data[production_data["Element"].astype(str).str.contains("Yield")]
    cropdb: pd.DataFrame = pd.read_csv(  # type: ignore
        os.path.join(DATA_PATH, "commodity_crosswalk.csv"))
    
    # # calculate some leakage
    coi_impacts_data = pd.read_csv(os.path.join(RPATH, COUNTRY_OF_INTEREST, "impacts_full.csv"), index_col = 0)
    coi_impacts_item = coi_impacts_data[coi_impacts_data.ItemT_Code == item_code]
    is_anim = coi_impacts_item.Animal_Product.str.contains("Primary").any()

    _internal = coi_impacts_item[coi_impacts_item["Producer_Country_Code"] == countries[COUNTRY_OF_INTEREST]]
    
    internal_bd_pm2 = _internal['bd_opp_cost_m2']
    internal_bd_pm2_err = _internal['opp_cost_err'] / 1E6

    internal_prov_tonnes = _internal['provenance']
    internal_land_use_pasture_m2_pkg = _internal['Pasture_m2'] / (1000 * internal_prov_tonnes)
    internal_land_use_arable_m2_pkg = _internal['Arable_m2'] / (1000 * internal_prov_tonnes)
    
    internal_yield_arable_kg_m2 = 1 / internal_land_use_arable_m2_pkg[internal_land_use_arable_m2_pkg > 0]
    internal_yield_pasture_kg_m2 = 1 / internal_land_use_pasture_m2_pkg[internal_land_use_pasture_m2_pkg > 0]

    coi_exports = fd.read_coi_exports(RPATH, ITEM_OF_INTEREST, 
                                      countries[COUNTRY_OF_INTEREST], 
                                      comm_of_interest_code=item_code)

    if len(coi_exports) == 0: 
        raise ValueError(
            f"Couldn't find any exports of {ITEM_OF_INTEREST} for {COUNTRY_OF_INTEREST}"
        )
    
    if is_anim:
        displaced_production_mass_kg = internal_yield_pasture_kg_m2.squeeze()
    else:
        displaced_production_mass_kg = internal_yield_arable_kg_m2.squeeze()

    coi_exports = coi_exports.reset_index()

    coi_exports["idisplaced_production_kg"] = displaced_production_mass_kg \
                * coi_exports["export_weight"] \
                * (coi_exports["import_ratio"] #/ np.nansum(coi_exports["import_ratio"])
                   )
    
    coi_exports["pasture_m2_per_kg"] = (coi_exports['Pasture_m2'] / (1000 * coi_exports['provenance'])).apply(fd.round_sig, args=(6,))
    coi_exports["arable_m2_per_kg"] = (coi_exports['Arable_m2'] / (1000 * coi_exports['provenance'])).apply(fd.round_sig, args=(6,))
    coi_exports["land_use_total_m2_per_kg"] = (coi_exports["arable_m2_per_kg"] + coi_exports["pasture_m2_per_kg"]).apply(fd.round_sig, args=(6,))

    coi_exports["bd_per_m2"] = coi_exports['bd_opp_cost_m2']
    coi_exports["bd_per_m2_err"] = coi_exports['opp_cost_err'] / 1E6

    coi_exports["swwu_l_per_kg"] = (coi_exports['SWWU_avg_calc'] / (1000 * coi_exports['provenance'])).fillna(0).apply(fd.round_sig, args=(6,))

    coi_exports["ghg_kg_co2eq_per_kg"] = (coi_exports['GHG_avg_calc'] / (1000 * coi_exports['provenance'])).fillna(0).apply(fd.round_sig, args=(6,))

    incl = ["Consumer_Country_Code", "Consumer_ISO3", # consumer
            "Producer_Country_Code", "Country_ISO", # producer
            "ItemT_Name", "ItemT_Code",
            "Item", "Item_Code",
            "pasture_m2_per_kg", 
            "arable_m2_per_kg", 
            "land_use_total_m2_per_kg",
            "bd_per_m2", "bd_per_m2_err",
            # "bd_per_kg", "bd_per_kg_err", 
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

    leakage_calc.to_csv("leakage_calc.csv", index=False)
    displaced_production_full.to_csv("displaced_production_full.csv", index=False)

    internal_rows = {
                    "Producer_ISO" : COUNTRY_OF_INTEREST,
                    "Producer_Country_Code" : countries[COUNTRY_OF_INTEREST],
                    "ItemT_Name" : ITEM_OF_INTEREST[0],
                    "ItemT_Code" : item_code,
                    "idisplaced_production_kg" : - displaced_production_mass_kg.squeeze(),
                    "pasture_m2_leakage" : - internal_land_use_pasture_m2_pkg.squeeze() * displaced_production_mass_kg.squeeze(),
                    "arable_m2_leakage" : - internal_land_use_arable_m2_pkg.squeeze() * displaced_production_mass_kg.squeeze(),
                    "land_use_total_m2_leakage" : -(internal_land_use_pasture_m2_pkg.squeeze() + internal_land_use_arable_m2_pkg.squeeze()) * displaced_production_mass_kg.squeeze(),
                    "bd_leakage" : - internal_bd_pm2.squeeze(), 
                    "bd_leakage_err" : _internal["opp_cost_err"].squeeze() / 1E6,
                    "swwu_l_leakage" : - _internal["SWWU_avg_calc"].squeeze() * displaced_production_mass_kg.squeeze() /  (1000 * internal_prov_tonnes),
                    "ghg_kg_co2eq_leakage" : - _internal["GHG_avg_calc"].squeeze() * displaced_production_mass_kg.squeeze() /  (1000 * internal_prov_tonnes),
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
        leakage_calc_per_kg[col] = leakage_calc_per_kg[col] / displaced_production_mass_kg.squeeze()
        leakage_calc_per_kg.rename(columns={col: f"{col}_per_kg"}, inplace=True) 

    leakage_calc_per_kg.to_csv("leakage_calc_kg.csv", index=False)

    return (leakage_calc, leakage_calc_per_kg, displaced_production_full)

if __name__ == "__main__":
    pass
    