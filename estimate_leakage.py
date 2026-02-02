import os
from typing import Any
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
import _funcs_data as fd
import _plot_funcs as pf
import faostat

# CONSTANTS
def calculate_leakage_vals(COUNTRY_OF_INTEREST, ITEM_OF_INTEREST, RPATH, 
                           LIFE_IMPACTS_KG_PATH, DATA_PATH):
    ccodes: pd.DataFrame = pd.read_csv(  # type: ignore
        os.path.join(DATA_PATH, "country_codes.csv"), encoding="latin-1"
    )
    
    countries: dict[str, int] = {}
    for dirname in os.listdir(RPATH):
        if os.path.isdir(os.path.join(RPATH, dirname)):
            countries[dirname.upper()] = ccodes.loc[
                ccodes["ISO3"] == dirname.upper(), "FAOSTAT"
            ].item()
    
    # VARIABLES TO BE SET BY THE USER DEPENDING ON WHAT THEY ARE LOOKING FOR
    # Careful with ITEM_A/ITEM_OF_INTEREST assignment - currently must be list and can only be certain
    # words (e.g. "Rape" works but not "Rapeseed")
    
    # Testing
    if COUNTRY_OF_INTEREST not in countries:
        print("Country not in set of countries")
        raise ValueError
    
    pdat: pd.DataFrame = faostat.get_data_df(
        "QCL",
        pars={"year": "2020",},
        strval=False
    )
    
    pdat = pdat[pdat["Year"] == pdat["Year"].unique().max()]
    
    yield_dat: pd.DataFrame = pdat[pdat["Element"].astype(str).str.contains("Yield")]
    
    cropdb: pd.DataFrame = pd.read_csv(  # type: ignore
        os.path.join(DATA_PATH, "commodity_crosswalk.csv"),
        ) 
    
    df: pd.DataFrame = pd.read_csv(LIFE_IMPACTS_KG_PATH, index_col=0)  # type: ignore
    
    # print(cropdb[cropdb.Item.isin(ITEM_OF_INTEREST)])
    is_anim = df[df.Item.isin(cropdb[cropdb.Item.isin(ITEM_OF_INTEREST)].group_name_v2)].isanim.any()
    
    # print(df[df.Item.isin(cropdb[cropdb.Item.isin(ITEM_OF_INTEREST)].group_name)])
    # quit()
    if is_anim: 
        internal_bd_kg, internal_bd_benefit, external_bd_leakage, internal_yield_kg_km2, leakage_calc, displaced_prod_full = fd.leakage_anims(
            df, yield_dat, cropdb, RPATH, (COUNTRY_OF_INTEREST, countries[COUNTRY_OF_INTEREST]), ITEM_OF_INTEREST
        )
        
        if len(internal_bd_kg) == 0:
            raise ValueError(
                f""
            )
        
        leakage_calc["AREA_RESTORE_KM2"] = np.nan
        

    else:
        internal_bd_kg, internal_bd_benefit, external_bd_leakage, internal_yield_kg_km2, leakage_calc, displaced_prod_full = fd.leakage_crops(
            df, yield_dat, cropdb, RPATH, (COUNTRY_OF_INTEREST, countries[COUNTRY_OF_INTEREST]), ITEM_OF_INTEREST
        )
        
        if len(internal_bd_kg) == 0:
            raise ValueError(
                f"No internal biodiversity value"
            )
            
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
    RPATH: str = "D:\\Food_v1\\all_results_v1" # where the outputs of the Food model are
    LIFE_IMPACTS_KG_PATH: str = "D:\\Food_v1\\country_bd_items_weights_v1.csv" # as above but the result of 'plot_global_commodity_impacts'
    RESULTS_DIR: str = "D:\\Leakage\\v1" # where to save stuff

    DATA_PATH: str = "data"
    
    COUNTRY_OF_INTEREST = "CRI"
    
    ITEM_OF_INTEREST = ['Meat; cattle']
    
    leakage_calc, displaced_prod_full = calculate_leakage_vals(COUNTRY_OF_INTEREST, ITEM_OF_INTEREST, RPATH, 
                            LIFE_IMPACTS_KG_PATH, DATA_PATH)
    