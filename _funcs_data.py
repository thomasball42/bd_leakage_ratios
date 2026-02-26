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
