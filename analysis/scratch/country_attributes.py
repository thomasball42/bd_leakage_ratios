# -*- coding: utf-8 -*-
"""
Country attributes to plot the leakage against: `python country_attributes.py [config.json]`.

  From the World Bank's API (downloaded once, into data/worldbank/):
    name, wb_region, income_group  its current regions and income groups (not
                                   those of the run year)
    gdp_pc                         GDP per capita, PPP (constant 2021 international $),
                                   NY.GDP.PCAP.PP.KD, in the run year
  From the run's FLMP trade matrix (TradeMatrix_import_dry_matter.csv), summed
  over every item, in its tonnes (which, despite the file's name, look like fresh
  weight: Brazil's production sums to 1.1 Gt, as in FAOSTAT and the portfolio, so
  heavy items such as sugar cane and milk carry most of the weight):
    consumption_t, production_t    the country's row and column sums
    domestic_t                     what it produces for itself, the diagonal
    self_sufficiency               domestic_t / consumption_t: the share of its
                                   consumption produced in the country
    production_ratio               production_t / consumption_t (> 1 for net exporters)

Written to data/country_attributes.csv, one row per ISO3.
"""

import argparse
import json
from pathlib import Path

import pandas as pd
import requests

import _common as c
import _leakage_funcs  # on the path from _run_paths
import _run_config

API = "https://api.worldbank.org/v2"
GDP = "NY.GDP.PCAP.PP.KD"
WB_DIR = c.DATA_DIR / "worldbank"

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("config", nargs="?", type=Path, default=c._run_paths.DEFAULT_CONFIG,
                    help=c._run_paths.CONFIG_HELP)
args = parser.parse_args()
config, _, _, data_dir = c._run_paths.run_paths(args.config)
year = config["run"]["year"]


def world_bank(name: str, url: str) -> list[dict]:
    """Every record of a World Bank API query, cached in data/worldbank/{name}.json."""
    path = WB_DIR / f"{name}.json"
    if not path.exists():
        WB_DIR.mkdir(parents=True, exist_ok=True)
        r = requests.get(url, params={"format": "json", "per_page": 20000}, timeout=60)
        r.raise_for_status()
        meta, records = r.json()
        if meta["pages"] != 1:
            raise ValueError(f"{url} has {meta['pages']} pages; raise per_page")
        path.write_text(json.dumps(records), encoding="utf-8")
    return json.loads(path.read_text(encoding="utf-8"))


countries = pd.DataFrame([
    {"ISO3": r["id"], "name": r["name"].strip(), "wb_region": r["region"]["value"].strip(),
     "income_group": r["incomeLevel"]["value"].strip()}
    for r in world_bank("countries", f"{API}/country") if r["region"]["value"] != "Aggregates"
]).set_index("ISO3")
countries["income_group"] = countries["income_group"].replace({"Not classified": None})
gdp = pd.Series({r["countryiso3code"]: r["value"]
                 for r in world_bank(f"{GDP}_{year}", f"{API}/country/all/indicator/{GDP}?date={year}")},
                name="gdp_pc")

trade_path = (_run_config.resolve(config["paths"]["flmp_results_dir"]) / str(year) / ".mrio"
              / "TradeMatrix_import_dry_matter.csv")
trade = pd.read_csv(trade_path, usecols=["Consumer_Country_Code", "Producer_Country_Code", "Value"])
isos = _leakage_funcs.load_country_isos(data_dir)
trade["consumer"] = trade["Consumer_Country_Code"].astype(int).map(isos)
trade["producer"] = trade["Producer_Country_Code"].astype(int).map(isos)
unmatched = trade["consumer"].isna() | trade["producer"].isna()
print(f"{trade_path}: {unmatched.mean():.2%} of the rows, {trade.loc[unmatched, 'Value'].sum() / trade['Value'].sum():.3%} "
      "of the tonnes, have a country with no ISO3, and are left out")
trade = trade[~unmatched]
supply = pd.DataFrame({
    "consumption_t": trade.groupby("consumer")["Value"].sum(),
    "production_t": trade.groupby("producer")["Value"].sum(),
    "domestic_t": trade[trade["consumer"] == trade["producer"]].groupby("consumer")["Value"].sum(),
}).fillna(0)
supply["self_sufficiency"] = supply["domestic_t"] / supply["consumption_t"]
supply["production_ratio"] = supply["production_t"] / supply["consumption_t"]

out = countries.join(gdp, how="outer").join(supply, how="outer")
out.index.name = "ISO3"
out = out[out.index.notna() & (out.index != "")]
out.to_csv(c.ATTRIBUTES_PATH)
print(f"Wrote {c.ATTRIBUTES_PATH}: {len(out)} countries, {out['gdp_pc'].notna().sum()} with GDP per capita in {year}, "
      f"{out['self_sufficiency'].notna().sum()} with self-sufficiency, {out['income_group'].notna().sum()} with an "
      "income group")
