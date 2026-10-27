# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 10:53:22 2024

@author: Thomas Ball

Market leakage of displaced production, computed for every country at once from
FLMP's MRIO trade matrix. For one item, with R[consumer, producer]:

    E = R / column sums     where each producer's output goes (export weights)
    S = R / row sums        where each consumer's supply comes from (import ratios)
    L = E^T S               L[C, p]: share of C's displaced production replaced by p

When C displaces m kg, producer p replaces L[C, p] * m kg. The land use, and the
impacts that follow it (LAND_DRIVEN), of each change are extensified: the change
at each land source - a crop's own land, an animal product's pasture, and each of
its feed crops where grown - goes through leakage_land_use with that source's
FLMP production and land and its beta, after netting the changes at each source
within C's scenario. The results are located where each source is, so feed land
counts where the feed is grown, not where the animal product is. The betas' uncertainty is carried by Monte Carlo: each draw
takes every beta estimate from Normal(beta, se) (see draw_betas).
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
# the land-driven impacts and their FLMP errors, scaled by each land source's extensification
SCALED = LAND_DRIVEN + [f"{col}_err" for col in LAND_DRIVEN]
# the extensified results, drawn in the Monte Carlo: land use and the land-driven impacts
EXTENSIFIED = ["land_use_m2"] + LAND_DRIVEN
# their names in the per-kg output
EXT_COLS = ["land_use_leakage_m2"] + [f"{col}_ext" for col in LAND_DRIVEN]
WORLD_CODE = 5000  # FAOSTAT area code of the World rows in data/beta_*.csv
# FLMP animal items that take the "All pasture-based animal products" row (Item Code 0)
# of data/beta_animals.csv: every animal product in FLMP's TradeMatrixFeed
PASTURE_ITEMS: set[int] = {867, 882, 947, 951, 977, 982, 1017, 1035, 1058, 1062, 1069, 1073, 1080, 1091, 1097,
                           1108, 1127, 1141, 1166}
DRAW_BATCH_BYTES = 2e8  # memory for each batch of draws in evaluate_extensified
# where the betas' filter had no data it leaves its prior (beta 0.5, se 1000): a beta row with an se above this is
# treated as none, as drawing from it gives betas in the thousands (the largest fitted se is about 3)
MAX_BETA_SE = 100


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


def load_intensities(path: Path, codes: np.ndarray) -> tuple[dict[int, tuple[np.ndarray, np.ndarray]],
                                                             dict[int, np.ndarray], dict[int, str]]:
    """Per-kg production impacts, and production, from FLMP's prod_impacts_long table.

    Returns, per item, a (country x VALUE_COLS) array on the `codes` index and a
    mask of the countries with no FLMP value, which are given the item's
    production-weighted world average instead; each item's production (kg) on
    the `codes` index, NaN where there's none; and the item names.
    """
    df = pd.read_csv(path)
    df = df[df["Effective_Producer_Code"].isin(codes)]
    sources = {out: (f"{stem}_per_kg", stem) for stem, out in IMPACTS.items()}
    sources.update({f"{out}_err": (f"{stem}_per_kg_err", f"{stem}_err") for stem, out in IMPACTS.items()})

    intensities, production = {}, {}
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
        production[int(item)] = np.full(len(codes), np.nan)
        production[int(item)][idx] = (g["production_tonnes"] * 1000).where(g["production_tonnes"] > 0).to_numpy()

    names = df.drop_duplicates("ItemT_Code").set_index("ItemT_Code")["ItemT_Name"]
    return intensities, production, {int(k): v for k, v in names.items()}


def pasture_based_production(intensities: dict[int, tuple[np.ndarray, np.ndarray]],
                             production: dict[int, np.ndarray], n_codes: int) -> np.ndarray:
    """Each country's FLMP production (kg) of the PASTURE_ITEMS that use pasture
    there: the scale of its pasture's curve, as its pasture beta is for all its
    pasture-based products together."""
    pasture = VALUE_COLS.index("pasture_m2")
    total = np.zeros(n_codes)
    for item in PASTURE_ITEMS & set(intensities) & set(production):
        total += np.where(intensities[item][0][:, pasture] > 0, np.nan_to_num(production[item]), 0)
    return total


def load_extensification(use_beta: bool, value: float | None, std: float | None, data_dir: Path,
                         beta_files: dict[str, str], year: int, codes: np.ndarray, items: list[int],
                         intensities: dict[int, tuple[np.ndarray, np.ndarray]],
                         ) -> tuple[np.ndarray, np.ndarray, dict[int, tuple[np.ndarray, np.ndarray]]]:
    """The beta estimates, and which one each producer of each item uses.

    The estimates are the rows of the "crops" and "animals" `beta_files` in
    data_dir (beta_crops.csv and beta_animals.csv by default) for the countries in
    `codes` and the World, with beta_{year} and se_{year} for the betas' `year` if
    `use_beta`, or otherwise `value` and `std` in place of every row's. Rows with
    an se above MAX_BETA_SE are left out, as unfitted.
    Returns each estimate's central beta and standard deviation,
    as arrays with a NaN appended (so estimate -1, none, gives NaN), and per item
    each producer's estimate on the `codes` index and a mask of the producers
    that use the item's World row as they have none of their own.

    PASTURE_ITEMS use their country's "All pasture-based animal products" row
    (Item Code 0 of the animals file), except where they have no pasture
    (feed-fed: pigs, poultry, eggs), which get none, as only their feed
    extensifies.
    """
    beta_col, se_col = f"beta_{year}", f"se_{year}"
    tables = []
    for name, item_filter in [(beta_files["crops"], None), (beta_files["animals"], 0)]:
        path = data_dir / name
        if not {beta_col, se_col} <= set(pd.read_csv(path, nrows=0).columns):
            raise ValueError(f"{path} has no {beta_col}/{se_col}, for the betas' year {year} "
                             f"(extensification.beta_year, or run.year where that is null)")
        table = pd.read_csv(path, usecols=lambda col: col in {"Area Code", "Item Code", "is_aggregate", beta_col,
                                                                  se_col})
        if "is_aggregate" in table:
            table = table[~table["is_aggregate"]]
        if item_filter is not None:
            table = table[table["Item Code"] == item_filter]
        tables.append(table)
    betas = pd.concat(tables, ignore_index=True)
    betas = betas[betas["Area Code"].isin(codes) | (betas["Area Code"] == WORLD_CODE)].reset_index(drop=True)
    unfitted = betas[se_col].to_numpy(float) > MAX_BETA_SE
    if unfitted.any():
        print(f"Treating {unfitted.sum()} beta rows with se_{year} above {MAX_BETA_SE} (the filter's unfitted prior) "
              f"as none: (Area Code, Item Code) {list(zip(betas.loc[unfitted, 'Area Code'], betas.loc[unfitted, 'Item Code']))}")
    beta_hat = betas[beta_col].to_numpy(float) if use_beta else np.full(len(betas), float(value))
    beta_se = betas[se_col].to_numpy(float) if use_beta else np.full(len(betas), float(std))
    usable = np.isfinite(beta_hat) & ~unfitted

    pasture = VALUE_COLS.index("pasture_m2")
    estimates = {}
    for item in items:
        rows = betas.index[(betas["Item Code"] == (0 if item in PASTURE_ITEMS else item)) & usable]
        areas = betas.loc[rows, "Area Code"].to_numpy()
        idx = np.full(len(codes), -1)
        own = areas != WORLD_CODE
        idx[np.searchsorted(codes, areas[own])] = rows[own]
        fallback = idx < 0
        world = rows[~own]
        if len(world):
            idx[fallback] = world[0]
        else:
            fallback[:] = False
        if item in PASTURE_ITEMS and item in intensities:
            no_pasture = intensities[item][0][:, pasture] <= 0
            idx[no_pasture] = -1
            fallback[no_pasture] = False
        estimates[item] = (idx, fallback)
    return np.append(beta_hat, np.nan), np.append(beta_se, np.nan), estimates


def draw_betas(beta_hat: np.ndarray, beta_se: np.ndarray, clipping: tuple[float | None, float | None] | None,
               draws: int, seed: int) -> dict[str, np.ndarray | None]:
    """The central betas, and `draws` sets of betas each drawn from
    Normal(beta_hat, beta_se), every estimate independently, from `seed`; both
    clipped to `clipping` ((lower, upper), either None for no bound; None for no
    clipping). Returns "central" (estimates), "se" (estimates) and "draws"
    (draws x estimates, or None), over the estimates from load_extensification.
    The same draws serve every item and displacement size.
    """
    lower, upper = (None, None) if clipping is None else clipping
    if lower is not None and upper is not None and lower > upper:
        raise ValueError(f"extensification.clipping's lower bound is above its upper bound: {clipping}")

    def clip(beta):
        return beta if lower is None and upper is None else np.clip(beta, lower, upper)

    z = np.random.default_rng(seed).standard_normal((draws, len(beta_hat))) if draws else None
    return {"central": clip(beta_hat), "se": beta_se, "draws": clip(beta_hat + beta_se * z) if draws else None}


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


def land_sources(item: int, intensities: dict[int, tuple[np.ndarray, np.ndarray]], production: dict[int, np.ndarray],
                 pasture_based: np.ndarray, estimates: dict[int, tuple[np.ndarray, np.ndarray]],
                 feed_mix: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """The land behind a kg of `item` from each producer: its own (a crop's arable
    land, or an animal product's pasture) and, for animal products, each feed crop
    where it was grown.

    One row per producer x source, with producer_idx, the source (source_item and
    source_idx, the country it's in), its beta `estimate`, its kg per kg of item
    (kg_per_kg), its FLMP production (production_kg), land (land_use_m2) and land
    per kg of its own output (land_m2_per_kg), and the SCALED impacts per kg of
    item that come from it. A crop source's land is
    its FLMP arable land per kg times its production. An animal product's pasture
    is on the scale of its producer's pasture-based production (`pasture_based`),
    so its land is its own pasture per kg times that. Feed is priced with each
    crop's impacts where it was grown, as in prod_impacts_long, so an animal
    product's own impacts (and errors, summed linearly) are its total less its
    feed's.
    """
    arable, pasture = VALUE_COLS.index("arable_m2"), VALUE_COLS.index("pasture_m2")
    scaled = [VALUE_COLS.index(col) for col in SCALED]
    intensity = intensities[item][0]
    if item in PASTURE_ITEMS:
        own_production, own_land_per_kg = pasture_based, intensity[:, pasture]
    else:
        own_production, own_land_per_kg = production[item], intensity[:, arable]
    own = pd.DataFrame({
        "producer_idx": np.arange(len(intensity)),
        "source_item": item,
        "source_idx": np.arange(len(intensity)),
        "estimate": estimates[item][0],
        "kg_per_kg": 1.0,
        "production_kg": own_production,
        "land_use_m2": own_land_per_kg * own_production,
        "land_m2_per_kg": own_land_per_kg,
    })
    own[SCALED] = intensity[:, scaled]
    if item not in feed_mix:
        return own

    feed_sources = []
    for feed_item, g in feed_mix[item].groupby("feed_item"):
        origin = g["origin_idx"].to_numpy()
        kg = g["feed_kg_per_kg"].to_numpy()
        crop_production = production[feed_item][origin] if feed_item in production else np.full(len(g), np.nan)
        if feed_item in intensities:
            per_kg = intensities[feed_item][0][origin]
            land_per_kg, impacts = per_kg[:, arable], kg[:, None] * per_kg[:, scaled]
        else:  # unpriced in prod_impacts_long too
            land_per_kg, impacts = np.full(len(g), np.nan), np.zeros((len(g), len(scaled)))
        source = pd.DataFrame({
            "producer_idx": g["producer_idx"].to_numpy(),
            "source_item": feed_item,
            "source_idx": origin,
            "estimate": estimates[feed_item][0][origin],
            "kg_per_kg": kg,
            "production_kg": crop_production,
            "land_use_m2": land_per_kg * crop_production,
            "land_m2_per_kg": land_per_kg,
        })
        source[SCALED] = impacts
        feed_sources.append(source)
    feed_sources = pd.concat(feed_sources, ignore_index=True)
    feed_impacts = feed_sources.groupby("producer_idx")[SCALED].sum()
    own[SCALED] -= feed_impacts.reindex(own["producer_idx"], fill_value=0).to_numpy()
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


def prepare_extensified(sources: pd.DataFrame, row_coi: np.ndarray, row_producer: np.ndarray, kg: np.ndarray,
                        row_replacement: np.ndarray, beta_central: np.ndarray) -> dict:
    """The parts of extensifying each scenario's changes that don't depend on the
    betas, for evaluate_extensified, given the item's land_sources and its rows:
    `kg` more (or, if negative, less) of the item from the producer at
    `row_producer`, in the scenario of the country of interest at `row_coi`, as
    a replacement (`row_replacement`) or displaced row.

    Within a scenario, the changes at each land source (a producer's own land, or
    a feed crop where it was grown) are netted first - C's loss against what it
    replaces itself, and feed origins shared between producers - and the net
    change goes through leakage_land_use once, as A * expm1(beta * log1p(net / Q))
    with the source's FLMP production Q and land A. Each row's kg at the source
    then gets the source's land use per kg of that net change, and its SCALED
    impacts times the source's extensified over linear land change for the net
    change. The results are summed by flow (displaced or replacement) and by where
    each source is, its location: a crop's land is where it's grown, an animal
    product's pasture where the animal is, and its feed where the feed was grown.
    A source with no land (an animal product's pasture, where it has none)
    changes nothing. Sources with no beta (`beta_central`, over the estimates, is
    NaN) or no FLMP production are left out. A flow is NaN in a scenario in which
    a source would lose more than it produces; so is the displaced flow where C's
    own sources are all left out, and the replacement flow where every replacing
    producer's are.

    Returns the scenarios (their row_coi); the rows to report, one per scenario x
    flow x location where the flow has impacts or production, sorted by scenario,
    flow (displaced, replacement) then location: their scenario (row_s, its
    position in scenarios), flow (row_f, 1 for replacement) and location (row_loc,
    on the codes index), production change (production_kg), linear changes
    (`linear`, at beta 1: land, then SCALED, then pasture land) and whether they're
    NaN (row_nan); and the NaN flows (flow_nan, flows x scenarios). The rest is for
    evaluate_extensified: the entries, each scenario x flow x source that changes,
    by row, with what's needed to extensify them.
    """
    # each source once
    key = sources["source_idx"].to_numpy(np.int64) * 1_000_000 + sources["source_item"].to_numpy(np.int64)
    keys, first, source_id = np.unique(key, return_index=True, return_inverse=True)
    estimate, production, land_use, location, source_item = (
        sources[col].to_numpy()[first] for col in ["estimate", "production_kg", "land_use_m2", "source_idx",
                                                   "source_item"])
    pasture = np.isin(source_item, list(PASTURE_ITEMS))  # an animal product's own source is its pasture

    # each source's kg, land (m2) and SCALED impacts, per kg of item from each producer
    producer = sources["producer_idx"].to_numpy()
    n_codes = producer.max() + 1
    per_kg = np.zeros((n_codes, len(keys)))
    np.add.at(per_kg, (producer, source_id), sources["kg_per_kg"].to_numpy())
    weights = np.zeros((1 + len(SCALED), n_codes, len(keys)))
    np.add.at(weights[0], (producer, source_id), (sources["kg_per_kg"] * sources["land_m2_per_kg"]).to_numpy())
    for j, col in enumerate(SCALED):
        np.add.at(weights[1 + j], (producer, source_id), sources[col].to_numpy())
    weights = np.nan_to_num(weights, nan=0, posinf=0, neginf=0)

    # each flow's kg of item from each producer, and the net change at each source, by scenario
    scenarios, scenario = np.unique(row_coi, return_inverse=True)
    flow = row_replacement.astype(int)
    item_kg = np.zeros((2, len(scenarios), n_codes))  # displaced, replacement
    np.add.at(item_kg, (flow, scenario, row_producer), kg)
    net = item_kg.sum(axis=0) @ per_kg
    with np.errstate(divide="ignore", invalid="ignore"):
        log_growth = np.log1p(net / production)
        production_over_net = production / net
    marginal = net == 0
    no_land = land_use == 0
    usable = np.isfinite(beta_central[estimate]) & (production > 0) & np.isfinite(land_use)
    # log_growth is -inf where a source loses all its production, which frees all its land
    known = usable & (np.isfinite(log_growth) | marginal | (log_growth == -np.inf))
    impossible = ((net < -production) & ~no_land).any(axis=1)

    # a row is covered if any of its producer's sources is known (or has no land)
    covered = (((known | no_land).astype(np.float32) @ (per_kg > 0).T.astype(np.float32)) > 0)[scenario, row_producer]
    flow_covered = np.zeros((2, len(scenarios)), bool)
    np.logical_or.at(flow_covered, (flow, scenario), covered)
    flow_nan = ~flow_covered | impossible

    # each flow's linear change at each source (flows x columns x scenarios x sources): its rows' kg times their
    # producers' weights, which the extensified change scales by the source's extensified over linear change
    changes = item_kg[:, None] @ weights[None]

    # the rows, keyed by scenario, flow and location: wherever a flow changes a source, or production
    n_scenarios = len(scenarios)
    entry_f, entry_s, entry_source = np.nonzero((changes != 0).any(axis=1))
    entry_key = (entry_s * 2 + entry_f) * n_codes + location[entry_source]
    production_f, production_s, production_loc = np.nonzero(item_kg)
    row_keys = np.union1d(entry_key, (production_s * 2 + production_f) * n_codes + production_loc)
    row_s, row_f, row_loc = row_keys // (2 * n_codes), row_keys // n_codes % 2, row_keys % n_codes

    # the entries, by row, and where each row's start (for the rows that have any)
    order = np.argsort(entry_key, kind="stable")
    entry_f, entry_s, entry_source, entry_key = entry_f[order], entry_s[order], entry_source[order], entry_key[order]
    segment_keys, segment_starts = np.unique(entry_key, return_index=True)
    entry_changes = changes[entry_f, :, entry_s, entry_source]  # entries x columns
    linear = np.zeros((len(row_keys), 2 + len(SCALED)))
    linear[np.searchsorted(row_keys, segment_keys)] = np.add.reduceat(
        np.column_stack([entry_changes, entry_changes[:, 0] * pasture[entry_source]]), segment_starts, axis=0)
    at = (entry_s, entry_source)
    return {
        "scenarios": scenarios, "flow_nan": flow_nan,
        "row_s": row_s, "row_f": row_f, "row_loc": row_loc, "row_nan": flow_nan[row_f, row_s],
        "production_kg": item_kg[row_f, row_s, row_loc], "linear": linear,
        "segment_rows": np.searchsorted(row_keys, segment_keys), "segment_starts": segment_starts,
        "no_entries": ~np.isin(row_keys, segment_keys),
        "estimate": estimate[entry_source], "marginal": marginal[at], "contributes": (known & ~no_land)[at],
        "changes": entry_changes, "changes32": entry_changes[:, :len(EXTENSIFIED)].astype(np.float32),
        "log_growth": log_growth[at], "production_over_net": production_over_net[at],
        "log_growth32": log_growth[at].astype(np.float32),
        "production_over_net32": production_over_net[at].astype(np.float32),
    }


def evaluate_extensified(prepared: dict, betas: np.ndarray, errors: bool = False) -> np.ndarray:
    """Each row's extensified land use (m2) and LAND_DRIVEN impacts - and with
    `errors` their FLMP errors, in SCALED order after them - for each set of betas
    in `betas` (estimates x sets, or one set), from prepare_extensified: the sum of
    its entries' linear changes, each scaled by its source's extensified over
    linear change. Returns a (columns x rows x sets) array, in float64 with
    `errors` (the central values) and float32 without (the draws), NaN for the
    rows prepare_extensified marks NaN."""
    p = prepared
    betas = betas[:, None] if betas.ndim == 1 else betas
    if errors:
        changes, log_growth, production_over_net, dtype = (p["changes"], p["log_growth"], p["production_over_net"],
                                                           np.float64)
    else:
        changes, log_growth, production_over_net, dtype = (p["changes32"], p["log_growth32"],
                                                           p["production_over_net32"], np.float32)
    # entries x sets, so each row's entries are summed over contiguous memory; in place where it can be, as new
    # memory (fresh pages) can be many times slower to write to than reused memory
    beta = betas[p["estimate"]].astype(dtype, copy=False)
    ratio = beta * log_growth[:, None]
    with np.errstate(invalid="ignore", over="ignore"):
        np.expm1(ratio, out=ratio)
        ratio *= production_over_net[:, None]
    np.copyto(ratio, beta, where=p["marginal"][:, None])
    ratio[~p["contributes"]] = 0
    del beta

    # each column's rows x sets is contiguous, which keeps writing the rows fast
    out = np.empty((changes.shape[1], len(p["row_s"]), betas.shape[1]), dtype)
    out[:, p["no_entries"]] = 0
    if len(p["segment_starts"]):
        product = np.empty_like(ratio)
        for j in range(changes.shape[1]):
            np.multiply(ratio, changes[:, j, None], out=product)
            out[j][p["segment_rows"]] = np.add.reduceat(product, p["segment_starts"], axis=0)
    out[:, p["row_nan"]] = np.nan
    return out


def replacement_rows(L: np.ndarray, coi_idx: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """The rows for the countries of interest at `coi_idx`: one 'replacement' row
    per producer p with L[C, p] > 0 and then one 'displaced' row per C. Returns
    each row's scenario (its position in `coi_idx`), its producer (on the codes
    index), its kg per kg displaced in C (L[C, p], or -1), and whether it's a
    replacement row."""
    rows_c, producer_idx = np.nonzero(L[coi_idx] > 0)
    scenario = np.concatenate([rows_c, np.arange(len(coi_idx))])
    producer = np.concatenate([producer_idx, coi_idx])
    share = np.concatenate([L[coi_idx[rows_c], producer_idx], -np.ones(len(coi_idx))])
    return scenario, producer, share, np.arange(len(scenario)) < len(rows_c)


def _add_spread(table: pd.DataFrame, name: str, central: np.ndarray, draws: np.ndarray | None,
                percentiles: list[float]) -> None:
    """Add `name` (the central values) to `table`, then with draws its percentiles
    (name_p{q}) and standard deviation (name_sd) over the draws (axis 0)."""
    table[name] = central
    if draws is not None:
        for q, values in zip(percentiles, np.percentile(draws, percentiles, axis=0)):
            table[f"{name}_p{q:g}"] = values
        table[f"{name}_sd"] = draws.std(axis=0, ddof=1)


def leakage_for_item(L: np.ndarray, intensity: np.ndarray, fallback: np.ndarray, coi_idx: np.ndarray,
                     sources: pd.DataFrame, displaced_kg: np.ndarray, estimates: tuple[np.ndarray, np.ndarray],
                     betas: dict, percentiles: list[float], production: np.ndarray,
                     pasture_based: np.ndarray | None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Leakage of displacing `displaced_kg` (one per country of interest) of the
    item in each country of interest at `coi_idx`, given L, its per-kg
    `intensity` and its fallback mask, its land_sources, each producer's beta
    estimate and World-fallback mask (`estimates`), the betas from draw_betas,
    and its FLMP `production` (kg) and, for animal products, each producer's
    `pasture_based` production, all on the codes index.

    Returns three tables, each with coi_idx (and the first two location_idx):
    - main: for each country of interest C, one 'displaced' row for each location
      where C's lost production had its land (C itself, and for an animal product
      the origins of its feed), and one 'replacement' row for each location where
      the replacing production has its land or is produced, with: the location's
      production change of the item, as a producer (L[C, location] * kg
      displaced, or minus it in C's displaced row, 0 where it only grows feed);
      the extensified land use and LAND_DRIVEN impacts located there (central,
      plus percentiles and sd over the draws) and their FLMP errors; production
      GHG of its production change (linear); and its extensification factor (its
      pasture's, for an animal product) and current FLMP production and land for
      the item;
    - per_kg: the same rows per kg displaced, with the linear impacts located
      there (the per-kg output);
    - summary: per country of interest, for each EXTENSIFIED result, the amount
      spared (minus its displaced rows' sum), leaked (its replacement rows' sum),
      the net leakage (leaked - spared) and their ratio, each central plus
      percentiles and sd, and production GHG spared and leaked, with its current
      state.
    """
    scenario, producer, share, replacement = replacement_rows(L, coi_idx)
    kg = share * displaced_kg[scenario]
    prepared = p = prepare_extensified(sources, coi_idx[scenario], producer, kg, replacement, betas["central"])
    central = evaluate_extensified(prepared, betas["central"], errors=True)[..., 0]  # columns x rows
    draws = None  # columns x rows x draws
    if betas["draws"] is not None:
        batch = max(1, int(DRAW_BATCH_BYTES // (4 * prepared["log_growth"].size)))
        draws_t = np.ascontiguousarray(betas["draws"].T, dtype=np.float32)  # estimates x draws
        draws = np.concatenate([evaluate_extensified(prepared, draws_t[:, i:i + batch])
                                for i in range(0, draws_t.shape[1], batch)], axis=-1)

    # the rows: each scenario's displaced locations, then its replacement ones
    row_s, row_f, location = p["row_s"], p["row_f"], p["row_loc"]
    scenario_kg = np.zeros(len(p["scenarios"]))
    scenario_kg[np.searchsorted(p["scenarios"], coi_idx)] = displaced_kg
    m = scenario_kg[row_s]
    production_change = p["production_kg"]

    col = VALUE_COLS.index
    estimate, ext_fallback = estimates
    current_production = production[location]
    main = pd.DataFrame({
        "coi_idx": p["scenarios"][row_s],
        "location_idx": location,
        "flow": np.where(row_f == 1, "replacement", "displaced"),
        "displaced_kg": m,
        "production_change_kg": production_change,
    })
    for j, name in enumerate(EXTENSIFIED):
        _add_spread(main, name, central[j], None if draws is None else draws[j].T, percentiles)
        if name in LAND_DRIVEN:
            main[f"{name}_err"] = np.abs(central[len(EXTENSIFIED) + LAND_DRIVEN.index(name)])
    main["ghg_prod_kgco2e"] = production_change * intensity[location, col("ghg_prod_kgco2e")]
    # FLMP's error takes the sign of its value, so it's negative where production GHG is
    main["ghg_prod_kgco2e_err"] = np.abs(production_change * intensity[location, col("ghg_prod_kgco2e_err")])
    main["extensification"] = betas["central"][estimate[location]]
    main["extensification_std"] = betas["se"][estimate[location]]
    main["fallback_intensity"] = fallback[location]
    main["fallback_extensification"] = ext_fallback[location]
    main["production_kg"] = current_production
    main["arable_m2_current"] = intensity[location, col("arable_m2")] * current_production
    main["pasture_m2_current"] = intensity[location, col("pasture_m2")] * current_production
    main["land_use_current_m2"] = main["arable_m2_current"] + main["pasture_m2_current"]
    main["pasture_based_production_kg"] = np.nan if pasture_based is None else pasture_based[location]

    # the linear changes located there, per kg displaced: land (arable, and pasture), the SCALED impacts with
    # their errors (summed linearly), and production GHG at the location as a producer
    linear = p["linear"] / m[:, None]
    pasture_m2 = linear[:, -1]
    per_kg = main[["coi_idx", "location_idx", "flow", "displaced_kg"]].copy()
    per_kg["displaced_production_kg"] = production_change / m
    per_kg[["extensification", "extensification_std"]] = main[["extensification", "extensification_std"]].to_numpy()
    for j, name in enumerate(EXT_COLS):
        per_kg[name] = central[j] / m
    per_kg["arable_m2"] = linear[:, 0] - pasture_m2
    per_kg["pasture_m2"] = pasture_m2
    for j, name in enumerate(SCALED):
        per_kg[name] = np.abs(linear[:, 1 + j]) if name.endswith("_err") else linear[:, 1 + j]
    per_kg["ghg_prod_kgco2e"] = main["ghg_prod_kgco2e"].to_numpy() / m
    per_kg["ghg_prod_kgco2e_err"] = main["ghg_prod_kgco2e_err"].to_numpy() / m
    per_kg = per_kg[["coi_idx", "location_idx", "flow", "displaced_kg", "displaced_production_kg", "extensification",
                     "extensification_std", *EXT_COLS, *VALUE_COLS]]
    per_kg["land_use_total_m2"] = per_kg["arable_m2"] + per_kg["pasture_m2"]
    per_kg[["fallback_intensity", "fallback_extensification"]] = main[["fallback_intensity",
                                                                       "fallback_extensification"]].to_numpy()

    # per country of interest: spared is minus the sum of its displaced rows, leaked the sum of its replacement rows
    position = np.searchsorted(p["scenarios"], coi_idx)
    summary = pd.DataFrame({"coi_idx": coi_idx, "displaced_kg": displaced_kg, "production_kg": production[coi_idx],
                            "land_use_current_m2": intensity[coi_idx][:, [col("arable_m2"), col("pasture_m2")]].sum(axis=1)
                            * production[coi_idx],
                            "pasture_based_production_kg": np.nan if pasture_based is None else pasture_based[coi_idx]})

    # the rows are sorted by scenario then flow, so each scenario x flow's are contiguous
    groups, group_starts = np.unique(row_s * 2 + row_f, return_index=True)

    def spared_leaked(values):  # (rows x sets) -> spared, leaked (sets x countries of interest)
        totals = np.zeros((2 * len(p["scenarios"]), values.shape[1]), values.dtype)
        totals[groups] = np.add.reduceat(values, group_starts, axis=0)
        totals = totals.reshape(-1, 2, values.shape[1])
        totals[p["flow_nan"].T] = np.nan
        return -totals[position, 0].T, totals[position, 1].T

    for j, name in enumerate(EXTENSIFIED):
        spared, leaked = spared_leaked(central[j][:, None])
        spared_draws, leaked_draws = spared_leaked(draws[j]) if draws is not None else (None, None)
        with np.errstate(divide="ignore", invalid="ignore"):
            for part, value, value_draws in [
                ("spared", spared, spared_draws), ("leaked", leaked, leaked_draws),
                ("net", leaked - spared, None if draws is None else leaked_draws - spared_draws),
                ("ratio", leaked / np.where(spared != 0, spared, np.nan),
                 None if draws is None else leaked_draws / np.where(spared_draws != 0, spared_draws, np.nan)),
            ]:
                _add_spread(summary, f"{name}_{part}", value[0], value_draws, percentiles)
        if name in LAND_DRIVEN:
            spared_err, leaked_err = spared_leaked(np.abs(central[len(EXTENSIFIED) + LAND_DRIVEN.index(name)])[:, None])
            summary[f"{name}_err_spared"], summary[f"{name}_err_leaked"] = -spared_err[0], leaked_err[0]
    for name in ["ghg_prod_kgco2e", "ghg_prod_kgco2e_err"]:
        totals = np.zeros((2, len(p["scenarios"])))
        np.add.at(totals, (row_f, row_s), main[name].to_numpy())
        # spared is minus the displaced rows' sum, so it's negative where the item's production GHG is; the errors are
        # positive in every row (above), so theirs is the sum
        spared = totals[0, position] if name.endswith("_err") else -totals[0, position]
        summary[f"{name}_spared"], summary[f"{name}_leaked"] = spared, totals[1, position]
    summary["replaced_share"] = np.bincount(scenario[replacement], share[replacement], minlength=len(coi_idx))
    summary["fallback_share"] = np.bincount(scenario[replacement], (share * fallback[producer])[replacement],
                                            minlength=len(coi_idx))
    return main, per_kg, summary


def per_m2_of_spared_land(per_kg: pd.DataFrame) -> pd.DataFrame:
    """Rescale the per-kg leakage to per m2 of land spared by the country of
    interest (the linear arable + pasture per kg of its displaced rows, wherever
    that land is, feed land included), EXT_COLS too."""
    keys = ["COUNTRY_OF_INTEREST_CODE", "ItemT_Code", "size"]
    spared = (per_kg.loc[per_kg["flow"] == "displaced"].groupby(keys, as_index=False)["land_use_total_m2"].sum()
              .rename(columns={"land_use_total_m2": "_spared_m2"}))
    out = per_kg.merge(spared, on=keys, how="left")
    scale = -out.pop("_spared_m2").where(lambda s: s != 0)
    for col in ["displaced_production_kg", "land_use_total_m2"] + EXT_COLS + VALUE_COLS:
        out[col] = out[col] / scale
    return out
