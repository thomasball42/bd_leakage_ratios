# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 10:53:22 2024

@author: Thomas Ball

Market leakage of displaced production, computed for every country at once from
FLMP's MRIO trade matrix. For one item, with R[consumer, producer]:

    E = R / column sums     where each producer's output goes (export weights)
    S = R / row sums        where each consumer's supply comes from (import ratios)
    L = E^T S               L[C, p]: share of C's displaced production replaced by p

The leakage per kg displaced in C is then sum_p L[C, p] * I[p] - I[C], where I is
the per-kg production impact of each producer (feed included for animal products).

Land use and the impacts that follow it (LAND_DRIVEN) are also given extensified
(EXT_COLS), on the displaced and replacement rows alike: the change in each land
source - a crop's own land, an animal product's pasture, and each of its feed
crops where grown - goes through leakage_land_use with that source's
extensification factor.
"""
from pathlib import Path

import numpy as np
import pandas as pd

# column stem in FLMP's prod_impacts_long table -> output column
IMPACTS = {
    "arable_area_m2_calc": "arable_m2",
    "pasture_area_m2_calc": "pasture_m2",
    "life_extinctions_per_sp_calc": "bd",
    "ghg_prod_kgco2e_calc": "ghg_prod_kgco2e",
    "ghg_coc_kgco2e_calc": "ghg_coc_kgco2e",
}
# outputs whose errors are carried through (summed linearly)
WITH_ERR = ["bd", "ghg_prod_kgco2e", "ghg_coc_kgco2e"]
VALUE_COLS = list(IMPACTS.values()) + [f"{col}_err" for col in WITH_ERR]
# impacts that follow land use, and so are scaled with it when extensified
LAND_DRIVEN = ["bd", "ghg_coc_kgco2e"]
# extensified land use and LAND_DRIVEN impacts, from extensified
EXT_COLS = ["land_use_leakage_m2"] + [f"{col}_ext" for col in LAND_DRIVEN]
SUMMARY_COLS = ["arable_m2", "pasture_m2", "land_use_total_m2", "bd", "ghg_prod_kgco2e", "ghg_coc_kgco2e"] + EXT_COLS
# per producer inputs to leakage_land_use, from load_extensification
EXTENSIFICATION_COLS = ["extensification", "extensification_std", "current_production_kg", "current_land_use_m2"]
WORLD_CODE = 5000  # FAOSTAT area code of the World rows in data/beta_*.csv
# FLMP animal items that take the "All pasture-based animal products" row (Item Code 0)
# of data/beta_animals.csv: every animal product in FLMP's TradeMatrixFeed
PASTURE_ITEMS: set[int] = {867, 882, 947, 951, 977, 982, 1017, 1035, 1058, 1062, 1069, 1073, 1080, 1091, 1097,
                           1108, 1127, 1141, 1166}


def load_country_isos(data_dir: Path) -> dict[int, str]:
    """FAOSTAT country code -> ISO3."""
    codes = pd.read_csv(data_dir / "country_codes.csv", encoding="utf-8-sig", usecols=["ISO3", "FAOSTAT"])
    codes = codes.dropna().drop_duplicates(subset="FAOSTAT")
    return dict(zip(codes["FAOSTAT"].astype(int), codes["ISO3"]))


def load_item_names(data_dir: Path) -> dict[int, str]:
    """FAOSTAT item code -> item name."""
    crosswalk = pd.read_csv(data_dir / "commodity_crosswalk.csv", usecols=["Item", "Item_Code"])
    crosswalk = crosswalk.dropna().drop_duplicates(subset="Item_Code")
    return dict(zip(crosswalk["Item_Code"].astype(int), crosswalk["Item"]))


def load_trade_matrices(results_dir: Path, year: int) -> tuple[np.ndarray, dict[int, np.ndarray]]:
    """FLMP's MRIO result as one dense consumer x producer matrix (tonnes) per
    item, all on the same sorted index of country codes."""
    path = Path(results_dir) / str(year) / ".mrio" / "TradeMatrix_import_dry_matter.csv"
    tm = pd.read_csv(path, usecols=["Consumer_Country_Code", "Producer_Country_Code", "Item_Code", "Value"])
    tm = tm[tm["Value"] > 0]

    consumers = tm["Consumer_Country_Code"].astype(int).to_numpy()
    producers = tm["Producer_Country_Code"].astype(int).to_numpy()
    items = tm["Item_Code"].astype(int).to_numpy()
    values = tm["Value"].to_numpy()

    codes = np.union1d(consumers, producers)
    consumer_idx = np.searchsorted(codes, consumers)
    producer_idx = np.searchsorted(codes, producers)

    matrices = {}
    for item in np.unique(items):
        rows = items == item
        R = np.zeros((len(codes), len(codes)))
        np.add.at(R, (consumer_idx[rows], producer_idx[rows]), values[rows])
        matrices[int(item)] = R
    return codes, matrices


def replacement_shares(R: np.ndarray, comprehensive: bool = True, exclude_self: bool = False) -> np.ndarray:
    """L[C, p]: share of country C's displaced production that is replaced by
    producer p. Rows sum to 1 for every country that produces the item, bar a
    country that consumes none of what it produces (when not comprehensive) or
    is the item's only producer (when excluding self).

    comprehensive: every consumer of C's production (C included) replaces its
        share from its own supply mix. Otherwise only C's own consumers do,
        i.e. L[C] = S[C].
    exclude_self: C can't replace its own displaced production, so C is dropped
        from each consumer's supply mix and the rest renormalised. Consumers that
        were supplied by C alone turn to the world production mix (without C).
    """
    production = R.sum(axis=0)
    supply = R.sum(axis=1)
    produces = production > 0

    if comprehensive:
        E = np.divide(R, production, out=np.zeros_like(R), where=produces)
    else:
        E = np.diag(produces.astype(float))

    if not exclude_self:
        S = np.divide(R, supply[:, None], out=np.zeros_like(R), where=supply[:, None] > 0)
        return E.T @ S

    # W[c, C] = E[c, C] / supply of c from everyone but C
    alt_supply = supply[:, None] - R
    W = np.divide(E, alt_supply, out=np.zeros_like(R), where=alt_supply > 1e-9 * supply[:, None])
    L = W.T @ R
    np.fill_diagonal(L, 0)

    unallocated = np.where(produces, 1 - L.sum(axis=1), 0)
    unallocated[unallocated < 1e-9] = 0
    world = np.tile(production, (len(production), 1))
    np.fill_diagonal(world, 0)
    world_total = world.sum(axis=1, keepdims=True)
    world = np.divide(world, world_total, out=np.zeros_like(world), where=world_total > 0)
    return L + unallocated[:, None] * world


def load_intensities(path: Path, codes: np.ndarray) -> tuple[dict[int, tuple[np.ndarray, np.ndarray]], dict[int, str]]:
    """Per-kg production impacts from FLMP's prod_impacts_long table.

    Returns, per item, a (country x VALUE_COLS) array on the `codes` index and a
    mask of the countries with no FLMP value, which are given the item's
    production-weighted world average instead. Also returns the item names.
    """
    df = pd.read_csv(path)
    df = df[df["Effective_Producer_Code"].isin(codes)]
    sources = {out: (f"{stem}_per_kg", stem) for stem, out in IMPACTS.items()}
    sources.update({f"{out}_err": (f"{stem}_per_kg_err", f"{stem}_err") for stem, out in IMPACTS.items()})

    intensities = {}
    for item, g in df.groupby("ItemT_Code"):
        idx = np.searchsorted(codes, g["Effective_Producer_Code"].astype(int))
        values = np.full((len(codes), len(VALUE_COLS)), np.nan)
        world = np.zeros(len(VALUE_COLS))
        for j, col in enumerate(VALUE_COLS):
            per_kg, total = sources[col]
            values[idx, j] = g[per_kg].to_numpy()
            usable = (g["production_tonnes"] > 0) & np.isfinite(g[total])
            kg = g.loc[usable, "production_tonnes"].sum() * 1000
            world[j] = g.loc[usable, total].sum() / kg if kg > 0 else np.nan

        main = [VALUE_COLS.index(col) for col in IMPACTS.values()]
        fallback = np.isnan(values[:, main]).any(axis=1)
        values[fallback] = world
        # the odd undefined (inf upstream) error also takes the world value
        values = np.where(np.isnan(values), world, values)
        intensities[int(item)] = (values, fallback)

    names = df.drop_duplicates("ItemT_Code").set_index("ItemT_Code")["ItemT_Name"]
    return intensities, {int(k): v for k, v in names.items()}


def load_extensification(value: float | str, std: float, data_dir: Path, year: int, codes: np.ndarray,
                         items: list[int], intensities: dict[int, tuple[np.ndarray, np.ndarray]],
                         ) -> dict[int, tuple[np.ndarray, np.ndarray]]:
    """Each producer's extensification factor, its standard deviation, and its
    current production (kg) and land use (m2) of the item, for leakage_land_use.

    Production and land use are the current_* columns of data/beta_crops.csv, or
    of data/beta_animals.csv for PASTURE_ITEMS. Returns, per item, a
    (country x EXTENSIFICATION_COLS) array on the `codes` index and a mask of the
    countries given the item's World row instead: for the factor when theirs is
    missing, and for production and land use when either is missing or zero.
    For PASTURE_ITEMS the land use is then the item's own pasture per kg, from
    `intensities`, times that production, as the row in beta_animals.csv covers
    all pasture-based products (feed-fed ones, with no pasture, get none).

    value: a float, used with `std` for every producer and item, or "beta" for
        the per country x item betas (beta_{year} and se_{year}) in the same
        files, `std` then unused. Negative betas are set to 0.
    """
    if isinstance(value, str) and value != "beta":
        raise ValueError(f"EXTENSIFICATION_VALUE must be a float or 'beta', not {value!r}")

    year_cols = {f"beta_{year}", f"se_{year}"}
    crops = pd.read_csv(data_dir / "beta_crops.csv",
                        usecols=lambda col: col in year_cols or not col.startswith(("beta_", "se_")))
    crops = crops[~crops["is_aggregate"]]
    crops["current_land_use_m2"] = crops["current_area_harvested_ha"] * 1e4
    animals = pd.read_csv(data_dir / "beta_animals.csv",
                          usecols=lambda col: col in year_cols or not col.startswith(("beta_", "se_")))
    animals["current_land_use_m2"] = animals["current_area_harvested_1000 ha"] * 1e7
    betas = pd.concat([crops, animals], ignore_index=True)
    betas = pd.DataFrame({
        "area": betas["Area Code"],
        "item": betas["Item Code"],
        "extensification": betas[f"beta_{year}"].clip(lower=0) if value == "beta" else float(value),
        "extensification_std": betas[f"se_{year}"] if value == "beta" else float(std),
        "current_production_kg": betas["current_production_t"] * 1000,
        "current_land_use_m2": betas["current_land_use_m2"],
    })

    extensification = {}
    for item in items:
        rows = betas[betas["item"] == (0 if item in PASTURE_ITEMS else item)]
        world = rows.loc[rows["area"] == WORLD_CODE, EXTENSIFICATION_COLS].to_numpy()
        world = world[0] if len(world) else np.full(len(EXTENSIFICATION_COLS), np.nan)
        rows = rows[rows["area"].isin(codes)]

        values = np.full((len(codes), len(EXTENSIFICATION_COLS)), np.nan)
        values[np.searchsorted(codes, rows["area"])] = rows[EXTENSIFICATION_COLS].to_numpy()
        if value != "beta":
            values[:, :2] = value, std  # producers with no row too
        no_factor = np.isnan(values[:, :2]).any(axis=1)
        no_amounts = ~(values[:, 2:] > 0).all(axis=1)
        values[no_factor, :2] = world[:2]
        values[no_amounts, 2:] = world[2:]
        if item in PASTURE_ITEMS and item in intensities:
            values[:, 3] = intensities[item][0][:, VALUE_COLS.index("pasture_m2")] * values[:, 2]
        extensification[item] = (values, no_factor | no_amounts)
    return extensification


def load_feed_mix(path: Path, codes: np.ndarray) -> dict[int, pd.DataFrame]:
    """Feed by crop and where it was grown, in kg per kg of each animal product, by
    the country that produced it, from _build_bd_prod_impacts_matrix's
    feed_mix_long table.

    Returns, per animal item, a table of producer_idx and origin_idx (on the
    `codes` index), feed_item and feed_kg_per_kg.
    """
    df = pd.read_csv(path)
    known = df["Effective_Producer_Code"].isin(codes) & df["Feed_Origin_Code"].isin(codes)
    if not known.all():
        print(f"Dropping {(~known).mean():.2%} of feed mix rows, with a producer or origin not in the trade matrix")
    df = df[known]
    return {
        int(item): pd.DataFrame({
            "producer_idx": np.searchsorted(codes, g["Effective_Producer_Code"]),
            "feed_item": g["Feed_Item_Code"].astype(int).to_numpy(),
            "origin_idx": np.searchsorted(codes, g["Feed_Origin_Code"]),
            "feed_kg_per_kg": g["feed_kg_per_kg"].to_numpy(),
        })
        for item, g in df.groupby("ItemT_Code")
    }


def land_sources(item: int, intensities: dict[int, tuple[np.ndarray, np.ndarray]],
                 extensification: dict[int, tuple[np.ndarray, np.ndarray]],
                 feed_mix: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """The land behind a kg of `item` from each producer: its own (a crop's land,
    or an animal product's pasture) and, for animal products, each feed crop where
    it was grown.

    One row per producer x source, with producer_idx, the source's kg per kg of
    item (kg_per_kg), its EXTENSIFICATION_COLS, and the LAND_DRIVEN impacts per kg
    of item that come from it. Feed is priced with each crop's impacts where it
    was grown, as in prod_impacts_long, so an animal product's own impacts are its
    total less its feed's.
    """
    land_cols = [VALUE_COLS.index(col) for col in LAND_DRIVEN]
    own = pd.DataFrame(extensification[item][0], columns=EXTENSIFICATION_COLS)
    own.insert(0, "producer_idx", np.arange(len(own)))
    own.insert(1, "kg_per_kg", 1.0)
    own[LAND_DRIVEN] = intensities[item][0][:, land_cols]
    if item not in feed_mix:
        return own

    feed_sources = []
    for feed_item, g in feed_mix[item].groupby("feed_item"):
        source = pd.DataFrame(extensification[feed_item][0][g["origin_idx"]], columns=EXTENSIFICATION_COLS)
        source.insert(0, "producer_idx", g["producer_idx"].to_numpy())
        source.insert(1, "kg_per_kg", g["feed_kg_per_kg"].to_numpy())
        if feed_item in intensities:
            per_kg = intensities[feed_item][0][g["origin_idx"]][:, land_cols]
        else:  # unpriced in prod_impacts_long too
            per_kg = np.zeros((len(g), len(land_cols)))
        source[LAND_DRIVEN] = g["feed_kg_per_kg"].to_numpy()[:, None] * per_kg
        feed_sources.append(source)
    feed_sources = pd.concat(feed_sources, ignore_index=True)
    feed_impacts = feed_sources.groupby("producer_idx")[LAND_DRIVEN].sum()
    own[LAND_DRIVEN] -= feed_impacts.reindex(own["producer_idx"], fill_value=0).to_numpy()
    return pd.concat([own, feed_sources], ignore_index=True)


def leakage_land_use(beta: np.ndarray, current_production: np.ndarray, current_land_use: np.ndarray, kg_displaced: np.ndarray) -> np.ndarray:
    """Land use (m2) of the extra production arriving at each replacing producer.

    beta: the producer's extensification factor for the item
    current_production: the producer's current production of the item (kg)
    current_land_use: the producer's current land use for the item (m2)
    kg_displaced: the extra production arriving at the producer, in kg per kg
        displaced in the country of interest (negative for production lost)

    ###If beta is 1, growth in land use is linear with production growth.
    """
    # current_land_use * (((kg_displaced + current_production) / current_production) ** beta - 1), without
    # the rounding error of 1 + kg_displaced / current_production when that ratio is tiny (kg into 1e9+ kg)
    return current_land_use * np.expm1(beta * np.log1p(kg_displaced / current_production))


def extensified(sources: pd.DataFrame, producer_idx: np.ndarray, kg: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Land use (m2) and LAND_DRIVEN impacts of `kg` more (or, if negative, less)
    of the item from each producer at `producer_idx`, given its land_sources.

    Each source's land use comes from leakage_land_use, and its impacts are scaled
    by the same curve: its extensified over its linear land change (~ its beta at
    these sizes). Sources with no extensification data are left out, and a row
    with none at all is NaN.
    """
    land = np.full(len(producer_idx), np.nan)
    impacts = np.full((len(producer_idx), len(LAND_DRIVEN)), np.nan)
    by_producer = dict(tuple(sources.groupby("producer_idx")))
    for p in np.unique(producer_idx):
        rows = np.nonzero(producer_idx == p)[0]
        src = by_producer[p]
        beta = src["extensification"].to_numpy()
        production = src["current_production_kg"].to_numpy()
        source_kg = kg[rows, None] * src["kg_per_kg"].to_numpy()
        source_land = leakage_land_use(beta, production, src["current_land_use_m2"].to_numpy(), source_kg)
        ratio = leakage_land_use(beta, production, production, source_kg) / source_kg
        known_land, known_ratio = np.isfinite(source_land), np.isfinite(ratio)
        land[rows] = np.where(known_land.any(axis=1), np.where(known_land, source_land, 0).sum(axis=1), np.nan)
        impacts[rows] = kg[rows, None] * (np.where(known_ratio, ratio, 0) @ src[LAND_DRIVEN].to_numpy())
        impacts[rows[~known_ratio.any(axis=1)]] = np.nan
    return land, impacts


def leakage_for_item(L: np.ndarray, intensity: np.ndarray, fallback: np.ndarray, coi_idx: np.ndarray,
                     extensification: np.ndarray, extensification_fallback: np.ndarray,
                     sources: pd.DataFrame) -> pd.DataFrame:
    """Leakage per kg displaced, for the countries at index `coi_idx`.

    One 'replacement' row per producer p with L[C, p] > 0 (its share times its
    per-kg impacts), and one 'displaced' row with C's own impacts negated.
    `extensification` (country x EXTENSIFICATION_COLS) and its fallback mask
    are on the `codes` index, from load_extensification, and `sources` is the
    item's land_sources. Both kinds of row also get their producer's
    extensification factor and its std (for an animal product, its pasture's),
    and EXT_COLS: land use and LAND_DRIVEN impacts with every land source's
    change extensified (see extensified).
    """
    rows_c, producer_idx = np.nonzero(L[coi_idx] > 0)
    country_idx = coi_idx[rows_c]
    share = L[country_idx, producer_idx]
    row_producer = np.concatenate([producer_idx, coi_idx])
    kg = np.concatenate([share, -np.ones(len(coi_idx))])
    land_use, ext_impacts = extensified(sources, row_producer, kg)

    displaced = -intensity[coi_idx]
    err_cols = [VALUE_COLS.index(f"{col}_err") for col in WITH_ERR]
    displaced[:, err_cols] *= -1  # errors stay positive

    out = pd.DataFrame({
        "coi_idx": np.concatenate([country_idx, coi_idx]),
        "producer_idx": row_producer,
        "flow": ["replacement"] * len(country_idx) + ["displaced"] * len(coi_idx),
        "displaced_production_kg": kg,
        "extensification": extensification[row_producer, 0],
        "extensification_std": extensification[row_producer, 1],
        "land_use_leakage_m2": land_use,
        "fallback_intensity": fallback[row_producer],
        "fallback_extensification": extensification_fallback[row_producer],
    })
    values = np.vstack([share[:, None] * intensity[producer_idx], displaced])
    for j, col in enumerate(VALUE_COLS):
        out[col] = values[:, j]
    for j, col in enumerate(LAND_DRIVEN):
        out[f"{col}_ext"] = ext_impacts[:, j]
    out["land_use_total_m2"] = out["arable_m2"] + out["pasture_m2"]
    return out


def per_m2_of_spared_land(leakage: pd.DataFrame) -> pd.DataFrame:
    """Rescale per-kg leakage to per m2 of land spared in the country of interest
    (its own arable + pasture per kg, feed land included), EXT_COLS too."""
    keys = ["COUNTRY_OF_INTEREST_CODE", "ItemT_Code"]
    spared = leakage.loc[leakage["flow"] == "displaced", keys + ["land_use_total_m2"]]
    spared = spared.rename(columns={"land_use_total_m2": "_spared_m2"})
    out = leakage.merge(spared, on=keys, how="left")
    scale = -out.pop("_spared_m2").where(lambda s: s != 0)
    for col in ["displaced_production_kg", "land_use_total_m2"] + EXT_COLS + VALUE_COLS:
        out[col] = out[col] / scale
    return out


def summarise(leakage: pd.DataFrame) -> pd.DataFrame:
    """Per country x item: impact spared at home, impact leaked elsewhere and
    their ratio (all per kg displaced), plus the share of the replacement that
    used world-average impacts."""
    keys = ["COUNTRY_OF_INTEREST", "COUNTRY_OF_INTEREST_CODE", "ItemT_Name", "ItemT_Code"]
    replacement = leakage[leakage["flow"] == "replacement"]
    leaked = replacement.groupby(keys)[SUMMARY_COLS].sum(min_count=1).add_suffix("_leaked")
    spared = -leakage[leakage["flow"] == "displaced"].set_index(keys)[SUMMARY_COLS].add_suffix("_spared")
    summary = spared.join(leaked, how="left")
    for col in SUMMARY_COLS:
        summary[f"{col}_ratio"] = summary[f"{col}_leaked"] / summary[f"{col}_spared"].where(lambda s: s != 0)
    summary["replaced_share"] = replacement.groupby(keys)["displaced_production_kg"].sum()
    summary["fallback_share"] = (
        replacement[replacement["fallback_intensity"]].groupby(keys)["displaced_production_kg"].sum()
    )
    summary["fallback_share"] = summary["fallback_share"].fillna(0)
    return summary.reset_index()
