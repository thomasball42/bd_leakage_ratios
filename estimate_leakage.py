import os
from typing import Any
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
import _funcs_data as fd
import _plot_funcs as pf
import faostat

# CONSTANTS
def calculate_leakage_vals_crops(COUNTRY_OF_INTEREST, ITEM_OF_INTEREST, RPATH, 
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
        pars={"year": "2020"}
    )
    
    pdat = pdat[pdat["Year"] == pdat["Year"].unique().max()]
    
    yield_dat: pd.DataFrame = pdat[pdat["Element"].astype(str).str.contains("Yield")]
    
    cropdb: pd.DataFrame = pd.read_csv(  # type: ignore
        os.path.join(DATA_PATH, "crop_db.csv"),
        ) 
    
    df: pd.DataFrame = pd.read_csv(LIFE_IMPACTS_KG_PATH, index_col=0)  # type: ignore
    
    internal_bd_kg, internal_bd_benefit, external_bd_leakage, internal_yield_kg_km2, leakage_calc, displaced_prod_full = fd.net_exporters(
        df, yield_dat, cropdb, RPATH, (COUNTRY_OF_INTEREST, countries[COUNTRY_OF_INTEREST]), ITEM_OF_INTEREST
    )
    
    # external_yield_kg_km2: Any = displaced_prod_full["FAO_yield_kgm2"] * 1E6 # kg/m2 to kg/km2
    # internal_bd_km2: list[np.ndarray] = [internal_bd_kg * internal_yield_kg_km2]
    # external_bd_km2: Any = (
    #     displaced_prod_full.groupby("Producer_Country_Code")["bd_opp_cost_m2"].mean().dropna() * 1E6 #E/m2 to E/km2
    # ) # national average bd per km2
    
    # insert row with benefit
    
    if len(internal_bd_kg) == 0:
        raise ValueError(
            f"No internal biodiversity value - probably this is an animal product"
        )
        
    leakage_calc.loc[len(leakage_calc)] = [fd.AREA_KM2,
                                           countries[COUNTRY_OF_INTEREST],
                                           COUNTRY_OF_INTEREST, 
                                
                                -displaced_prod_full.idisp_prod.sum(), 
                                float(internal_bd_kg), 
                                float(internal_bd_kg) * -displaced_prod_full.idisp_prod.sum()]
    
    leakage_calc["leakage_per_kg_halted_production"] = leakage_calc.bd_leakage / displaced_prod_full.idisp_prod.sum()
    # leakage_calc["bd_benefit"] = [_ for _ in leakage_calc.bd_leakage if _ < 0]    

    
    
    
    # leakage_calc["bd_per_kg"] = internal_bd
    # pf.leakage_yield_bd_boxes(ITEM_OF_INTEREST, internal_bd_benefit, external_bd_leakage, 
    #                        internal_yield_kg_km2, external_yield_kg_km2,
    #                        internal_bd_km2, external_bd_km2)
    
    
    return leakage_calc, displaced_prod_full

if __name__ == "__main__":
    RPATH: str = "E:\\Food_v1\\all_results_v1_birds" # where the outputs of the Food model are
    LIFE_IMPACTS_KG_PATH: str = "E:\\Food_v1\\country_bd_items_weights_v1_birds.csv" # as above but the result of 'plot_global_commodity_impacts'
    RESULTS_DIR: str = "E:\\Leakage\\v1_birds" # where to save stuff

    DATA_PATH: str = "data"
    
    COUNTRY_OF_INTEREST = "GBR"
    
    ITEM_OF_INTEREST = ["Cabbages"]
    
    leakage_calc, displaced_prod_full = calculate_leakage_vals_crops(COUNTRY_OF_INTEREST, ITEM_OF_INTEREST, RPATH, 
                            LIFE_IMPACTS_KG_PATH, DATA_PATH)
    