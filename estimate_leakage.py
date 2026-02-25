import os
from typing import Any
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
import _funcs_data as fd
import _plot_funcs as pf

# CONSTANTS
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

    # if displaced_production_mass_kg == 0: # catches the case where yield is None (i.e. mushrooms)
    #     raise ValueError(
    #         f"Displaced production calculated to be zero! (likely mushrooms?)"
    #     )
    
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

    # coi_exports["bd_per_kg"] = coi_exports['bd_opp_cost_calc'] / (1000 * coi_exports['provenance'])

    coi_exports["swwu_l_per_kg"] = (coi_exports['SWWU_avg_calc'] / (1000 * coi_exports['provenance'])).apply(fd.round_sig, args=(6,))
    # coi_exports["swwu_l_per_kg_err"] = coi_exports['SWWU_avg_calc_err'] / (1000 * coi_exports['provenance'])

    coi_exports["ghg_kg_co2eq_per_kg"] = (coi_exports['GHG_avg_calc'] / (1000 * coi_exports['provenance'])).apply(fd.round_sig, args=(6,))
    # coi_exports["ghg_kg_co2eq_per_kg_err"] = coi_exports['GHG_avg_calc_err'] / (1000 * coi_exports['provenance'])
    
    incl = ["Country_ISO", "Producer_Country_Code", "Item", "Item_Code", 
            "pasture_m2_per_kg", 
            "arable_m2_per_kg", 
            "land_use_total_m2_per_kg",
            "bd_per_m2", "bd_per_m2_err",
            # "bd_per_kg", "bd_per_kg_err", 
            "swwu_l_per_kg",
            "ghg_kg_co2eq_per_kg", 
            "idisplaced_production_kg"]
    
    coi_exports = coi_exports[incl]

    agg_displacement = coi_exports.groupby(coi_exports.columns.drop("idisplaced_production_kg").to_list()).sum()

    agg_displacement.to_csv("agg_disp.csv")

    print(agg_displacement.idisplaced_production_kg.sum())

    # coi_exports["leakage_pasture_m2"] = coi_exports["pasture_m2_per_kg"] * coi_exports["idisplaced_production_kg"]
    # coi_exports["leakage_arable_m2"] = coi_exports["arable_m2_per_kg"] * coi_exports["idisplaced_production_kg"]
    # coi_exports["leakage_bd"] = coi_exports["bd_per_m2"] * (coi_exports["pasture_m2_per_kg"] + coi_exports["arable_m2_per_kg"]) * coi_exports["idisplaced_production_kg"]

    print(displaced_production_mass_kg)
    quit()

    # coi_impacts_data = pd.read_csv(os.path.join(RPATH, COUNTRY_OF_INTEREST, "impacts_full.csv"), index_col = 0)
    # coi_impacts_item = coi_impacts_data[coi_impacts_data.ItemT_Code == item_code]

    # _internal = coi_impacts_item[coi_impacts_item["Producer_Country_Code"] == countries[COUNTRY_OF_INTEREST]]
    # internal_bd_pm2 = _internal['bd_opp_cost_m2']
    # internal_prov_tonnes = _internal['provenance']
    # land_use_pasture_m2_pkg = _internal['Pasture_m2'] / (1000 * _internal['provenance'])
    # land_use_arable_m2_pkg = _internal['Arable_m2'] / (1000 * _internal['provenance'])
    
    # if is_anim: 
    #     internal_bd_kg, internal_bd_benefit, external_bd_leakage, internal_yield_kg_km2, leakage_calc, displaced_prod_full = fd.leakage_anims(
    #         df, yield_data, cropdb, RPATH, (COUNTRY_OF_INTEREST, countries[COUNTRY_OF_INTEREST]), ITEM_OF_INTEREST
    #     )
        
    #     if len(internal_bd_kg) == 0:
    #         raise ValueError(
    #             f""
    #         )
        
    #     leakage_calc["AREA_RESTORE_KM2"] = np.nan
        

    # else:
    #     internal_bd_kg, internal_bd_benefit, external_bd_leakage, internal_yield_kg_km2, leakage_calc, displaced_prod_full = fd.leakage_crops(
    #         df, yield_data, cropdb, RPATH, (COUNTRY_OF_INTEREST, countries[COUNTRY_OF_INTEREST]), ITEM_OF_INTEREST
    #     )
        
    #     if len(internal_bd_kg) == 0:
    #         raise ValueError(
    #             f"No internal biodiversity value"
    #         )
            
    leakage_calc.loc[len(leakage_calc)] = [fd.AREA_KM2,
                                           countries[COUNTRY_OF_INTEREST],
                                           COUNTRY_OF_INTEREST, 
                                -displaced_prod_full.idisp_prod.sum(), 
                                float(internal_bd_kg), 
                                float(internal_bd_kg) * -displaced_prod_full.idisp_prod.sum()]
    
    leakage_calc["leakage_per_kg_halted_production"] = leakage_calc.bd_leakage / displaced_prod_full.idisp_prod.sum()
    
    displaced_prod_full["idisp_arable_m2"] = displaced_prod_full.idisp_prod / displaced_prod_full.FAO_yield_kgm2
    displaced_prod_full["idisp_pasture_m2"] = displaced_prod_full.idisp_prod * displaced_prod_full.Pasture_m2.replace(0, np.nan) / (1000*displaced_prod_full.provenance)

    asum = (displaced_prod_full.groupby(["Producer_Country_Code", "Country_ISO"])["idisp_arable_m2"].sum()).to_frame().reset_index()
    asum.loc[len(asum)] = np.nan
    leakage_calc["idisp_arable_km2"] = asum.idisp_arable_m2 / 1E6
    
    psum = (displaced_prod_full.groupby(["Producer_Country_Code", "Country_ISO"])["idisp_pasture_m2"].sum()).to_frame().reset_index()
    psum.loc[len(asum)] = np.nan
    leakage_calc["idisp_pasture_km2"] = psum.idisp_pasture_m2 / 1E6
    
    leakage_calc["arable_km2_per_kg_halted_production"] = leakage_calc["idisp_arable_km2"] / displaced_prod_full.idisp_prod.sum()
    leakage_calc["pasture_km2_per_kg_halted_production"] = leakage_calc["idisp_pasture_km2"] / displaced_prod_full.idisp_prod.sum()
    
    return leakage_calc, displaced_prod_full

if __name__ == "__main__":
    pass
    