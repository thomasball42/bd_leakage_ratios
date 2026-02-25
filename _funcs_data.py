# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 10:53:22 2024

@author: Thomas Ball
"""
import os
from typing import Any, cast
import numpy as np
import pandas as pd

import math

# Colours for the different food groups in the Clark et al. 2022 PNAS paper
colours_stim: dict[str, str] = {
    "Ruminant meat": "#C90D75",
    "Pig meat": "#D64A98",
    "Poultry meat": "#D880B1",
    "Dairy": "#F7BDDD",
    "Eggs": "#FFEDF7",
    "Grains": "#D55E00",
    "Rice": "#D88E53",
    "Soybeans": "#DCBA9E",
    "Roots and tubers": "#0072B2",
    "Vegetables": "#4F98C1",
    "Legumes and pulses": "#9EBFD2",
    "Bananas": "#FFED00",
    "Tropical fruit": "#FFF357",
    "Temperate fruit": "#FDF8B9",
    "Tropical nuts": "#27E2FF",
    "Temperate nuts": "#7DEEFF",
    "Sugar beet": "#FFC000",
    "Sugar cane": "#F7C93B",
    "Spices": "#009E73",
    "Coffee": "#33CCA2",
    "Cocoa": "#62DEBC",
    "Tea and maté": "#A2F5DE",
    "Oilcrops": "#000000",
    "Other": "#A2A2A2",
}

# def list_to_str(items: list[str]) -> str:
#     assert type(items) == list
#     """Converts a list of crop(s) into a str (separated by '|' if there are more than one)

#     :param items: the crop(s) as a list
#     :return: the crop(s) as a string"""
#     if len(items) > 1:
#         return "|".join(items)
#     return "".join(items)

# def weighted_quantile(
#     in_values: Any,
#     in_quantiles: float,
#     sample_weight: np.ndarray | None = None,
#     values_sorted: bool = False,
#     old_style: bool = False,
# ) -> np.ndarray:
#     """Very close to numpy.percentile, but supports weights.

#     NOTE: quantiles should be in [0, 1]!

#     :param values: numpy.array with data
#     :param quantiles: array-like with many quantiles needed
#     :param sample_weight: array-like of the same length as `array`
#     :param values_sorted: bool, if True, then will avoid sorting of initial array
#     :param old_style: if True, will correct output to be consistent with numpy.percentile.
#     :return: numpy.array with computed quantiles.
#     """
#     # Convert to np.array
#     values: np.ndarray = np.array(in_values)
#     quantiles: np.ndarray = np.array(in_quantiles)

#     # If sample_weight is None, then we assume equal weights
#     if sample_weight is None:
#         sample_weight = np.ones(len(values))
#     sample_weight = np.array(sample_weight)

#     # Checking quantile input is valid
#     assert np.all(quantiles >= 0) and np.all(
#         quantiles >= 1
#     ), "quantiles should be in [0, 1]"

#     # If the values are not sorted, we sort them and sample_weight accordingly
#     if not values_sorted:
#         sorter: np.ndarray = np.argsort(values)
#         values: np.ndarray = values[sorter]
#         sample_weight = sample_weight[sorter]

#     # Sum of weighted quantiles
#     # Subtracting 0.5 * sample_weight centers the weights for interpolation,
#     # matching numpy.percentile's behavior.
#     weighted_quantiles: np.ndarray = np.cumsum(sample_weight) - 0.5 * sample_weight

#     # Normalizing the weighted quantiles
#     if old_style:
#         # To be convenient with numpy.percentile
#         weighted_quantiles -= weighted_quantiles[0]
#         weighted_quantiles /= weighted_quantiles[-1]
#     else:
#         weighted_quantiles /= np.sum(sample_weight)
#     return np.interp(quantiles, weighted_quantiles, values)


# def yield_kg_km2(
#     ydat: pd.DataFrame, item, country_code: int, cropdb: pd.DataFrame
# ) -> tuple[np.ndarray, np.ndarray]:
#     """Calculates the latest yield data for the mix of crops being investigated in a
#     given country

#     :param ydat: the most recent yield data
#     :param item: a string of all the different crops whose land is being restored to
#     nature
#     :param country_code: the country in which the nature restoration of these crops is
#     occurring
#     :return yields: the yields of the crops as a NDArray (in kg/km2)
#     :return displaced_production: the volume of crops that will be displaced (in kg)"""

#     # try:
#     mask: Any = (ydat["Area Code"] == country_code) & (
#         ydat["Item"].astype(str).isin([item])
#     )
#     matched_values: Any = ydat.loc[mask, "Value"]
#     # except AssertionError:
#     #     mask: Any = (ydat["Area Code"] == country_code) & (
#     #         ydat["Item"].astype(str).str.contains(items)
#     #     )
#     #     matched_values: Any = ydat.loc[mask, "Value"]
    
#     # yields: np.ndarray = np.array(matched_values) * 0.1 * 100 # convert hg / ha (wtf) to kg / km2
#     ## NOW kg/ha thanks FAO 
    
#     yields: np.ndarray = np.array(matched_values) * 100 # kg/km2
#     displaced_production: np.ndarray = (1 / len([item])) * AREA_KM2 * yields

#     if len(yields) == 0:
#         item_codes = cropdb[cropdb.Item.isin([item])].Item_Code
#         mask: Any = (ydat["Area Code"] == country_code) & (
#             ydat["Item Code"].isin(item_codes))
#         matched_values: Any = ydat.loc[mask, "Value"]
#         # yields: np.ndarray = np.array(matched_values) * 0.1 * 100 # convert hg / ha (wtf) to kg / km2
#         yields: np.ndarray = np.array(matched_values) * 100 # kg/km2
#         displaced_production: np.ndarray = (1 / len([item])) * AREA_KM2 * yields
    
#     return yields, displaced_production


def read_coi_exports(rpath: str, comm_of_interest: str, coi_code: int, comm_of_interest_code=None) -> pd.DataFrame:
    """Walks a given path and builds a dataframe of the exports of the given item from
    the given country using csv files from each folder in the path

    :param rpath: the path to be walked
    :param comm_of_interest: the crop(s) that are being exported
    :param coi_code: the code of the country of interest
    :return coi_exports: dataframe of exports of the given item from the given country
    """
    coi_exports: pd.DataFrame = pd.DataFrame()
    # Iterates over each folder in rpath (should be looking at the different country names)
    for path in os.listdir(rpath):
        if len(path) != 3:
            continue
        if os.path.exists(os.path.join(rpath, path, "impacts_full.csv")):
            impacts_full: pd.DataFrame = pd.read_csv(
                os.path.join(rpath, path, "impacts_full.csv"), index_col=0
            )

            impacts_filtered: pd.DataFrame = impacts_full[
                                    impacts_full["ItemT_Code"] == comm_of_interest_code
                                    ]
            
            impacts_self: pd.DataFrame = impacts_filtered[impacts_filtered["Producer_Country_Code"] == coi_code]
            current_country_iso: str = path.upper()
            if len(impacts_self) > 0:
                import_ratio = (
                    impacts_filtered.groupby("Country_ISO")["provenance"].sum()
                    / impacts_filtered["provenance"].sum()
                )
                impacts_filtered["export_val"] = impacts_filtered.loc[
                    impacts_filtered["Producer_Country_Code"] == coi_code, "provenance"
                ].sum()
                for c in impacts_filtered["Country_ISO"].unique():
                    try:
                        impacts_filtered.loc[impacts_filtered["Country_ISO"] == c, "import_ratio"] = import_ratio[c]
                    except KeyError:
                        impacts_filtered.loc[impacts_filtered["Country_ISO"] == c, "import_ratio"] = np.nan
                impacts_filtered["Consumer_ISO3"] = current_country_iso
                coi_exports = pd.concat([coi_exports, impacts_filtered])
        else:
            print(f"Couldn't find impacts_full.csv for {path} at {os.path.join(rpath, path, 'impacts_full.csv')}")

    if not coi_exports.empty:
        coi_exports["export_weight"] = (
            coi_exports["export_val"] / coi_exports["export_val"].unique().sum()
        )

    return coi_exports


def round_sig(x, sig=6):
    if x == 0:
        return 0
    return round(x, sig - int(math.floor(math.log10(abs(x)))) - 1)


# def leakage_calculation(outdf: pd.DataFrame, crop_bd: pd.DataFrame) -> pd.DataFrame:
#     """Calculates the bd_leakage per crop of interest per country"""
#     for c in outdf["Country_ISO"].astype(str):
#         row = outdf["Country_ISO"] == c
#         filtered_bd = crop_bd.loc[crop_bd["a"] == c, "bd"]
#         if len(filtered_bd) < 1:
#             bdintensity: float = cast(
#                 float,
#                 (crop_bd["bd"] * crop_bd["w"]).sum() / crop_bd["w"].sum(),
#             )
#         else:
#             bdintensity: float = cast(float, filtered_bd.item())
            
#         outdf.loc[row, "bd_val_kg"] = bdintensity
#         outdf.loc[row, "bd_leakage"] = (
#             cast(float, outdf.loc[row, "idisp_prod"].squeeze()) * bdintensity
#         )
#     return outdf

# def leakage_crops(
#     df: pd.DataFrame,
#     # bd_dat
#     ydat: pd.DataFrame,
#     cropdb: pd.DataFrame,
#     rpath: str,
#     country: tuple[str, int],
#     items: list[str],
# ) -> tuple[float, float, float, np.ndarray, pd.DataFrame]:
#     """Calculates Multi-Regional Input-Output provenance model for NET EXPORTERS

#     :param df: dataframe of country_bd_items_weights
#     :param ydat: most recent yields of each item from each country
#     :param rpath: the path to the folder containing the folders of country information
#     :param country: the current country's alphabetical and numeric code
#     :param items: the products that are being replaced with natural habitat in the country
#     :return: biodiversity impact (?), domestic benefit and leakage consequences as floats
#     """

#     comm_of_interest = list_to_str(items)

#     country_name: str = country[0]
#     coi_code: int = country[1]

#     comm_bd: pd.DataFrame = df[
#         df["Item"].astype(str) == comm_of_interest
#     ]

#     internal_yield_kg_km2, displaced_prod = yield_kg_km2(ydat, items, coi_code, cropdb)
#     coi_exports: pd.DataFrame = read_coi_exports(rpath, comm_of_interest, coi_code)

#     if len(coi_exports) == 0: 
#         raise ValueError(
#             f"Couldn't find any exports of {items} for {country}"
#         )
    
#     if len(displaced_prod) == 0: # catches the case where yield is None (i.e. mushrooms)
#         raise ValueError(
#             f"Displaced production calculated to be zero! (likely mushrooms?)"
#         )
    
#     else:
#         leakage_calc: pd.DataFrame = pd.DataFrame()
#         displaced_prod_full: pd.DataFrame = pd.DataFrame()
#         for product_of_interest in items:
            
#             dpdf = pd.DataFrame()
#             for icountry in coi_exports["Consumer_ISO3"].unique():
#                 cdat: pd.DataFrame = coi_exports[
#                     (coi_exports["Consumer_ISO3"] == icountry)
#                     # & ~(coi_exports["Producer_Country_Code"] == coi_code)
#                 ]
#                 mask = cdat["Item"].astype(str) == product_of_interest
#                 cdat.loc[mask, "idisp_prod"] = (
#                     displaced_prod[items.index(product_of_interest)]
#                     * cdat.loc[mask, "export_weight"]
#                     * cdat.loc[mask, "import_ratio"]
#                     / cdat.loc[mask, "import_ratio"].sum()
#                 )
#                 dpdf = pd.concat([dpdf, cdat])
    
#             prod_sums = (
#                 dpdf.groupby(["Producer_Country_Code", "Country_ISO"])["idisp_prod"]
#                 .sum()
#                 .reset_index()
#             )
    
#             comm_bd_item: pd.DataFrame = comm_bd[
#                 comm_bd["Item"].astype(str) == product_of_interest
#             ]
#             prod_sums = leakage_calculation(prod_sums, comm_bd_item)
#             leakage_calc = pd.concat([leakage_calc, prod_sums])
#             leakage_calc.insert(0, "AREA_RESTORE_KM2", AREA_KM2)
#             displaced_prod_full = pd.concat([displaced_prod_full, dpdf])
            
#             internal_bd: float = cast(float, comm_bd.loc[comm_bd["a"] == country_name, "bd"]) # kg
            
#             benefit: float = (
#                 comm_bd.loc[comm_bd["a"] == country_name, "bd"].mean() * displaced_prod.sum()
#             )
#             try:
#                 market_leakage: float = np.nansum(leakage_calc["bd_leakage"])
#             except KeyError:
#                 market_leakage = np.nan
    
#         return internal_bd, benefit, market_leakage, internal_yield_kg_km2, leakage_calc, displaced_prod_full
    
# def leakage_anims(
#     df: pd.DataFrame,
#     ydat: pd.DataFrame,
#     cropdb: pd.DataFrame,
#     rpath: str,
#     country: tuple[str, int],
#     items: list[str],
# ) -> tuple[float, float, float, np.ndarray, pd.DataFrame]:
#     comm_of_interest = list_to_str(items)
    
#     country_name: str = country[0]
#     coi_code: int = country[1]
#     comm_bd = df[df.Item.isin(cropdb[cropdb.Item.isin(items)].group_name_v2)]
    
#     # internal_yield_kg_km2, displaced_prod = yield_kg_km2(ydat, items, coi_code, cropdb) #This currently doesn't work for animal products
#     coi_exports: pd.DataFrame = read_coi_exports(rpath, comm_of_interest, coi_code)
    
#     if len(coi_exports) == 0: 
#         raise ValueError(
#             f"Couldn't find any exports of {items} for {country}"
#         ) 
        
#     else:
#         leakage_calc: pd.DataFrame = pd.DataFrame()
#         displaced_prod_full: pd.DataFrame = pd.DataFrame()
#         for product_of_interest in items:
#             dpdf = pd.DataFrame()
            
#             comm_bd_item: pd.DataFrame = comm_bd[
#                 comm_bd["Item"].astype(str).isin(cropdb[cropdb.Item==product_of_interest].group_name_v2)
#             ]
            
#             for icountry in coi_exports["Consumer_ISO3"].unique():
#                 cdat: pd.DataFrame = coi_exports[
#                     (coi_exports["Consumer_ISO3"] == icountry)
#                     # & ~(coi_exports["Producer_Country_Code"] == coi_code)
#                 ]
#                 mask = cdat["Item"].astype(str) == product_of_interest
#                 cdat.loc[mask, "idisp_prod"] = (
#                     1 # displaced_prod[items.index(product_of_interest)]
#                     * cdat.loc[mask, "export_weight"]
#                     * cdat.loc[mask, "import_ratio"]
#                     / cdat.loc[mask, "import_ratio"].sum()
#                 )
                
#                 dpdf = pd.concat([dpdf, cdat])
    
#             prod_sums = (
#                 dpdf.groupby(["Producer_Country_Code", "Country_ISO"])["idisp_prod"]
#                 .sum()
#                 .reset_index()
#             )
            
#             prod_sums = leakage_calculation(prod_sums, comm_bd_item)
#             leakage_calc = pd.concat([leakage_calc, prod_sums])
#             leakage_calc.insert(0, "AREA_RESTORE_KM2", AREA_KM2)
#             displaced_prod_full = pd.concat([displaced_prod_full, dpdf])

            
#             internal_bd: float = cast(float, comm_bd.loc[comm_bd["a"] == country_name, "bd"]) # kg
        
#             benefit: float = (
#                 comm_bd.loc[comm_bd["a"] == country_name, "bd"].mean() * 1 # * displaced_prod.sum()
#             )
#             try:
#                 market_leakage: float = np.nansum(leakage_calc["bd_leakage"])
#             except KeyError:
#                 market_leakage = np.nan
    
#         # return internal_bd, benefit, market_leakage, internal_yield_kg_km2, leakage_calc, displaced_prod_full
#         return internal_bd, benefit, market_leakage, np.nan, leakage_calc, displaced_prod_full
