# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 12:33:50 2025

@author: tom

This code is inexcusably inefficient I have no doubt, I will not apologise for art.
"""
import pandas as pd
import estimate_leakage
import os
# import faostat
from tqdm import tqdm
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore', message='Workbook contains no default style')


# CONSTANTS
OVERWRITE = True
RPATH: Path = Path("results", "2021") # where the outputs of the Food model are
LIFE_IMPACTS_KG_PATH: Path = Path("mrio_pipeline", "input_data", "mapspam_outputs", "outputs", "2020", "processed_results_2020.csv") # as above but the result of 'plot_global_commodity_impacts'
RESULTS_DIR: Path = Path("results_leakage", "2021") # where to save stuff
DATA_PATH: Path = Path("mrio_pipeline", "input_data")

if not RESULTS_DIR.is_dir():
    RESULTS_DIR.mkdir(exist_ok=True, parents=True)

cropdb: pd.DataFrame = pd.read_csv(  # type: ignore
    DATA_PATH / "commodity_crosswalk.csv",
    ) 

pdat = pd.read_csv(DATA_PATH / "Production_Crops_Livestock_E_All_Data_(Normalized).csv", encoding="latin-1", low_memory=False)
crops_pdat = pdat[(pdat.Element=="Production")&(pdat["Item Code"].isin(cropdb.Item_Code))]
anims_pdat = pdat[(pdat.Element=="Production")&(pdat["Item Code"].isin(cropdb[cropdb.SPAM_name.isin(["ruminant meat", "meat", "eggs"])].Item_Code))]

ccodes: pd.DataFrame = pd.read_excel(  # type: ignore
    DATA_PATH / "nocsDataExport_20251021-164754.xlsx")

countries: dict[str, int] = {}
for dirname in os.listdir(RPATH):
    if os.path.isdir(os.path.join(RPATH, dirname)) and len(dirname) == 3:
        countries[dirname.upper()] = ccodes.loc[
            ccodes["ISO3"] == dirname.upper(), "FAOSTAT"
        ].item()

countries = {
                "GBR" : 229, 
                # "CRI" : 48
             }

# item_code_list = set(crops_pdat["Item Code"].unique().tolist() + anims_pdat["Item Code"].unique().tolist())
item_code_list = [15]

with tqdm(total=len(countries) * len(item_code_list), 
          desc="Processing countries and items") as pbar:
    for COUNTRY_OF_INTEREST, country_code in countries.items():
        
        if not os.path.isfile(os.path.join(RESULTS_DIR, f"{COUNTRY_OF_INTEREST}_leakage.csv")) or OVERWRITE:
            cdf: pd.DataFrame = pd.DataFrame()
            
            for item_code in item_code_list:    
                
                db_entry = cropdb[cropdb.Item_Code == item_code]
                
                ITEM_OF_INTEREST: list[str] = db_entry.Item.to_list()

                pbar.update(1)
                pbar.set_postfix({"Country": COUNTRY_OF_INTEREST, "Item": ITEM_OF_INTEREST[0]})
                
                # check if country produces that item at all
                if len(crops_pdat[(crops_pdat["Area Code"] == country_code) & (crops_pdat["Item Code"] == item_code)]) < 1:
                    continue

                # try:
                leakage_calc, displaced_prod_full = estimate_leakage.return_leakage_df(COUNTRY_OF_INTEREST, ITEM_OF_INTEREST, RPATH,
                                        DATA_PATH, item_code, countries=countries, production_data=pdat)
                # except ValueError:
                #     leakage_calc = pd.DataFrame()
                
                leakage_calc.insert(0, "COUNTRY_OF_INTEREST", COUNTRY_OF_INTEREST)
                leakage_calc.insert(1, "COUNTRY_OF_INTEREST_CODE", country_code)
                leakage_calc.insert(2, "ITEM_OF_INTEREST", ITEM_OF_INTEREST[0])
     
                cdf = pd.concat([cdf, leakage_calc])
            
            cdf.to_csv(os.path.join(RESULTS_DIR, f"{COUNTRY_OF_INTEREST}_leakage.csv"))

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    