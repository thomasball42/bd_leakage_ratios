# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 12:33:50 2025

@author: tom

This code is inexcusably inefficient I have no doubt, I will not apologise for art.
"""
import pandas as pd
import estimate_leakage
import os
import faostat
from tqdm import tqdm
import numpy as np

# CONSTANTS
OVERWRITE = True
RPATH: str = "E:\\Food_v1\\all_results_v1" # where the outputs of the Food model are
LIFE_IMPACTS_KG_PATH: str = "E:\\Food_v1\\country_bd_items_weights_v1.csv" # as above but the result of 'plot_global_commodity_impacts'
RESULTS_DIR: str = "E:\\Leakage\\v1" # where to save stuff
DATA_PATH: str = "data"


if not os.path.isdir(RESULTS_DIR):
    os.mkdir(RESULTS_DIR)

# Fetch production data using faostat package
pdat: pd.DataFrame = faostat.get_data_df(
    "QCL",
    pars={"year": "2020",},
    strval=False
)

# bddat: pd.DataFrame = pd.read_csv(LIFE_IMPACTS_KG_PATH, index_col=0)

cropdb: pd.DataFrame = pd.read_csv(  # type: ignore
    os.path.join(DATA_PATH, "crop_db.csv"),
    ) 

crops_pdat = pdat[(pdat.Element=="Production")&(pdat["Item Code"].isin(cropdb.Item_Code))]
anims_pdat = pdat[(pdat.Element=="Production")&(pdat["Item Code"].isin(cropdb[cropdb.SPAM_name.isin(["ruminant meat", "meat", "eggs"])].Item_Code))]

ccodes: pd.DataFrame = pd.read_csv(  # type: ignore
    os.path.join(DATA_PATH, "country_codes.csv"), encoding="latin-1"
)

countries: dict[str, int] = {}
for dirname in os.listdir(RPATH):
    if os.path.isdir(os.path.join(RPATH, dirname)) and len(dirname) == 3:
        countries[dirname.upper()] = ccodes.loc[
            ccodes["ISO3"] == dirname.upper(), "FAOSTAT"
        ].item()

# countries = {
#                 "GBR" : 229, 
#                 # "CRI" : 48
#              }

item_code_list = set(crops_pdat["Item Code"].unique().tolist() + anims_pdat["Item Code"].unique().tolist())


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

                try:
                    leakage_calc, displaced_prod_full = estimate_leakage.calculate_leakage_vals(COUNTRY_OF_INTEREST, ITEM_OF_INTEREST, RPATH, 
                                            LIFE_IMPACTS_KG_PATH, DATA_PATH)
                except ValueError:
                    leakage_calc = pd.DataFrame()
                
                leakage_calc.insert(0, "COUNTRY_OF_INTEREST", COUNTRY_OF_INTEREST)
                leakage_calc.insert(1, "COUNTRY_OF_INTEREST_CODE", country_code)
                leakage_calc.insert(2, "ITEM_OF_INTEREST", ITEM_OF_INTEREST[0])
     
                cdf = pd.concat([cdf, leakage_calc])
            
            cdf.to_csv(os.path.join(RESULTS_DIR, f"{COUNTRY_OF_INTEREST}_leakage.csv"))

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    