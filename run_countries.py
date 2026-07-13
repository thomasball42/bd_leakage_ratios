# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 12:33:50 2025

@author: Tom B

This code is inexcusably inefficient I have no doubt, I will not apologise for art.
"""
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from tqdm import tqdm

import _leakage_funcs

import warnings
warnings.filterwarnings('ignore', message='Workbook contains no default style')


### Configuration variables ###
OVERWRITE = True
COUNTRIES = [
            "GBR"
            ]

COMPREHENSIVE = False # default: True. Whether to run for all exports of the item (True) or just the country of interest (False) valid when crops mainly imported or used domestically (e.g., the UK)
NO_ANIM = True # default: False. exclude animal products
SELF_EXCL = True # default: False. exclude COI from ratio calculation (i.e. 100% of leakage occurs externally) - this only works if COMPREHENSIVE is False

# Parallelism knobs. With COMPREHENSIVE=True, each process worker ends up
# caching every country's impacts_full.csv in its own memory, so memory use
# scales with N_COUNTRY_WORKERS x total_data_size - lower this if that mode
# is used over the full country list.
N_COUNTRY_WORKERS = min(len(COUNTRIES) or 1, os.cpu_count() or 4)
N_ITEM_THREADS = min(32, (os.cpu_count() or 4) * 4)

RESULTS_DIR: Path = Path("results_leakage", "2021") # where to save stuff
RPATH: Path = Path("mrio_pipeline_results", "2021") # where the outputs of the FLMP model pipeline are (mrio_pipeline)
QUIET = True # shhhh

# these should stay the same if 'mrio pipeline' is in the same parent dir as this (and you want to run 2020)
LIFE_IMPACTS_KG_PATH: Path = Path("mrio_pipeline", "input_data", "mapspam_outputs", "outputs", "2020", "processed_results_2020.csv")
DATA_PATH: Path = Path("mrio_pipeline", "input_data")


def process_country(COUNTRY_OF_INTEREST, country_code, item_code_list, cropdb,
                     crops_pdat, pdat, RPATH, RESULTS_DIR,
                     COMPREHENSIVE, NO_ANIM, SELF_EXCL, QUIET,
                     show_progress: bool) -> None:
    """Computes and writes the leakage results for a single country across
    all item codes. Each item is independent given the (cached) input data,
    so they're farmed out to a thread pool."""

    def _run_item(item_code):
        db_entry = cropdb[cropdb.Item_Code == item_code]
        ITEM_OF_INTEREST: list[str] = db_entry.Item.to_list()

        if len(crops_pdat[(crops_pdat["Area Code"] == country_code) & (crops_pdat["Item Code"] == item_code)]) < 1:
            return None

        return _leakage_funcs.return_leakage_df(
            COUNTRY_OF_INTEREST, ITEM_OF_INTEREST, RPATH,
            item_code, countries={COUNTRY_OF_INTEREST: country_code}, production_data=pdat,
            COMPREHENSIVE=COMPREHENSIVE, NO_ANIM=NO_ANIM, SELF_EXCL=SELF_EXCL,
            QUIET=QUIET)

    cdf_frames = []
    cdf_per_kg_frames = []

    with ThreadPoolExecutor(max_workers=N_ITEM_THREADS) as executor:
        futures = {executor.submit(_run_item, item_code): item_code for item_code in item_code_list}
        iterator = as_completed(futures)
        if show_progress:
            iterator = tqdm(iterator, total=len(futures), desc=f"Items for {COUNTRY_OF_INTEREST}")
        for future in iterator:
            leakage_results = future.result()
            if leakage_results:
                leakage_calc = leakage_results[0]
                leakage_calc_per_kg = leakage_results[1]

                leakage_calc.insert(0, "COUNTRY_OF_INTEREST", COUNTRY_OF_INTEREST)
                leakage_calc.insert(1, "COUNTRY_OF_INTEREST_CODE", country_code)

                cdf_frames.append(leakage_calc)
                cdf_per_kg_frames.append(leakage_calc_per_kg)

    if not cdf_frames:
        return

    cdf = pd.concat(cdf_frames)
    cdf_per_kg = pd.concat(cdf_per_kg_frames)

    comp_str = "" if COMPREHENSIVE else "NOEXP_"
    noanim_str = "NOANIM_" if NO_ANIM else ""
    self_excl_str = "SELFEXCL_" if SELF_EXCL else ""

    filename_str = f"{COUNTRY_OF_INTEREST}_{comp_str}{noanim_str}{self_excl_str}"

    cdf.to_csv(os.path.join(RESULTS_DIR, f"{filename_str}leakage.csv"), index=False)
    cdf_per_kg.to_csv(os.path.join(RESULTS_DIR, f"{filename_str}leakage_per_kg.csv"), index=False)


if __name__ == "__main__":

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

    if len(COUNTRIES) > 0:
        countries = {c.upper(): ccodes.loc[ccodes["ISO3"] == c.upper(), "FAOSTAT"].item() for c in COUNTRIES}
    else:
        countries: dict[str, int] = {}
        for dirname in os.listdir(RPATH):
            if os.path.isdir(os.path.join(RPATH, dirname)) and len(dirname) == 3:
                countries[dirname.upper()] = ccodes.loc[
                    ccodes["ISO3"] == dirname.upper(), "FAOSTAT"
                ].item()

    item_code_list = set(crops_pdat["Item Code"].unique().tolist() + anims_pdat["Item Code"].unique().tolist())

    pending = {
        country: code for country, code in countries.items()
        if OVERWRITE or not os.path.isfile(os.path.join(RESULTS_DIR, f"{country}_leakage.csv"))
    }

    if len(pending) <= 1:
        # Single country (the common case, e.g. today's COUNTRIES = ["GBR"]):
        # run in-process so item-level tqdm progress is preserved.
        for country, country_code in pending.items():
            process_country(country, country_code, item_code_list, cropdb,
                             crops_pdat, pdat, RPATH, RESULTS_DIR,
                             COMPREHENSIVE, NO_ANIM, SELF_EXCL, QUIET,
                             show_progress=True)
    else:
        # Multiple independent countries: parallelize across processes, each
        # writing its own output file. Item-level tqdm is disabled inside
        # workers to avoid clashing output; progress is tracked per-country.
        with ProcessPoolExecutor(max_workers=N_COUNTRY_WORKERS) as executor:
            futures = {
                executor.submit(process_country, country, country_code, item_code_list,
                                 cropdb, crops_pdat, pdat, RPATH, RESULTS_DIR,
                                 COMPREHENSIVE, NO_ANIM, SELF_EXCL, QUIET,
                                 show_progress=False): country
                for country, country_code in pending.items()
            }
            with tqdm(total=len(futures), desc="Processing countries") as pbar:
                for future in as_completed(futures):
                    future.result()
                    pbar.update(1)
