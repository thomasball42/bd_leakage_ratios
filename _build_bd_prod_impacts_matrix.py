"""
Builds a producer country x commodity matrix of the extinction opportunity cost
per kilogram of PRODUCTION (bd_opp_cost_calc / production_kg), plus a matching
matrix of its error, from the impacts_full.csv files for a given year.

Where build_bd_cons_impacts_matrix.py answers "what does a tonne of commodity X
consumed by country Y cost?", this answers "what does a kilogram of commodity X
*produced* in country Y cost?". It has two parts:

- direct: every row of every consumer country's impacts_full.csv is attributed to
  the country that grew it (Producer_Country_Code) and the item itself (Item_Code),
  feed rows included, giving the impact per kg of each crop, or of the pasture for
  an animal product, where it was produced;
- feed (animal products only): the feed embodied in each kg of an animal product
  produced in country Y, recovered from the MRIO trade matrices (see
  feed_per_tonne_produced) and priced with the direct impacts of each feed crop
  where it was grown.

This is done for every impact in impacts_full.csv (arable and pasture area,
extinctions, production GHG and carbon opportunity cost) and written to
prod_impacts_long_<year>.csv plus one item matrix per impact. The feed itself, by
crop and where it was grown, per kg of each animal product by producer, is written
to feed_mix_long_<year>.csv. Everything goes to --out-dir; nothing is written to
the FLMP results dir.
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

import _leakage_funcs

RESULTS_DIR = Path("../outputs/flmp_results/flmp_results_261009")
INPUT_DATA_DIR = Path("data")  # country_codes.csv and commodity_crosswalk.csv
YEAR = 2021

IMPACT_COLS = [
    "arable_area_m2_calc",
    "pasture_area_m2_calc",
    "life_extinctions_per_sp_calc",
    "ghg_prod_kgco2e_calc",
    "ghg_coc_kgco2e_calc",
]
# each impact and its error, e.g. arable_area_m2_calc, arable_area_m2_calc_err
IMPACT_AND_ERR_COLS = [c for col in IMPACT_COLS for c in (col, f"{col}_err")]
USECOLS = ["Producer_Country_Code", "Item_Code", "Item", "provenance_tonnes"] + IMPACT_AND_ERR_COLS
# each impact (and its error) is also split into the direct part (crop or pasture
# rows) and the feed part, e.g. arable_area_m2_calc_direct, arable_area_m2_calc_feed_err
SPLIT_COLS = [
    f"{col}_{part}{suffix}" for col in IMPACT_COLS for part in ("direct", "feed") for suffix in ("", "_err")
]
VALUE_COLS = IMPACT_AND_ERR_COLS + SPLIT_COLS + ["production_tonnes", "feed_tonnes"]
# feed mix entries at or below this (kg feed per kg animal product) are lstsq noise, dropped
# (three quarters of the entries, 2e-8 of the feed for 2021)
MIN_FEED_KG_PER_KG = 1e-9
GROUPING = "group_name_v6"


def build_direct_long(results_dir: Path, year: int) -> tuple[pd.DataFrame, dict]:
    """Sum impacts and tonnage by (item, producing country) over every row of every
    country's impacts_full.csv.

    Feed rows count towards the feed crop where it was grown (Item_Code,
    Producer_Country_Code), like food rows; primary animal product rows carry the
    pasture. So the per-kg values from these sums are the impacts of producing the
    item itself, and the tonnage is the production traced through the MRIO.
    """

    files = sorted((results_dir / str(year)).glob("*/impacts_full.csv"))
    if not files:
        raise FileNotFoundError(f"No impacts_full.csv files under {results_dir / str(year)}")

    item_names: dict[float, str] = {}
    rows = []
    for f in tqdm(files, desc=f"Reading impacts_full files for {year}"):
        df = pd.read_csv(f, usecols=USECOLS)
        item_names.update(df[["Item_Code", "Item"]].dropna().drop_duplicates().set_index("Item_Code")["Item"])
        rows.append(
            df.groupby(["Item_Code", "Producer_Country_Code"], as_index=False)[["provenance_tonnes"] + IMPACT_AND_ERR_COLS].sum()
        )

    direct = pd.concat(rows, ignore_index=True)
    direct = direct.groupby(["Item_Code", "Producer_Country_Code"], as_index=False)[["provenance_tonnes"] + IMPACT_AND_ERR_COLS].sum()
    direct = direct.astype({"Item_Code": int, "Producer_Country_Code": int})

    return direct, item_names


def read_mrio(results_dir: Path, year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """FLMP's MRIO trade matrix, and the animal product rows of its feed matrix."""

    mrio = results_dir / str(year) / ".mrio"
    trade = pd.read_csv(mrio / "TradeMatrix_import_dry_matter.csv",
                        usecols=["Consumer_Country_Code", "Producer_Country_Code", "Item_Code", "Value"])
    feed = pd.read_csv(mrio / "TradeMatrixFeed_import_dry_matter.csv",
                       usecols=["Producer_Country_Code", "Consumer_Country_Code", "Item_Code", "Value", "Animal_Product_Code"])
    feed = feed[feed["Animal_Product_Code"].notna() & (feed["Value"] > 0)]
    feed = feed.astype({"Producer_Country_Code": int, "Item_Code": int, "Animal_Product_Code": int})
    return trade, feed


def solve_per_producer(trade: pd.DataFrame, item: int, footprints: pd.DataFrame,
                       warn: bool = True) -> tuple[np.ndarray, np.ndarray]:
    """Solve R g = G for g_p, what each tonne of `item` produced in p embodies,
    where G (`footprints`, indexed by consumer country code) is what each
    consumer's consumption of the item embodies and R is the item's MRIO trade
    matrix. Returns g (producers x columns of G) and the producers' codes."""

    flows = trade[(trade["Item_Code"] == item) & (trade["Value"] > 0)]
    codes = np.union1d(np.union1d(flows["Consumer_Country_Code"], flows["Producer_Country_Code"]),
                       footprints.index).astype(int)
    R = np.zeros((len(codes), len(codes)))
    np.add.at(R, (np.searchsorted(codes, flows["Consumer_Country_Code"].astype(int)),
                  np.searchsorted(codes, flows["Producer_Country_Code"].astype(int))), flows["Value"].to_numpy())
    G = np.zeros((len(codes), footprints.shape[1]))
    G[np.searchsorted(codes, footprints.index.astype(int))] = footprints.to_numpy()

    producers = np.nonzero(R.sum(axis=0) > 0)[0]
    Rp = R[:, producers]
    g, _, rank, _ = np.linalg.lstsq(Rp, G, rcond=None)
    if warn:
        residual = np.linalg.norm(Rp @ g - G) / np.linalg.norm(G)
        if residual > 1e-6:
            print(f"Warning: item {int(item)}: feed footprints only fit to a relative residual of {residual:.1e}")
        if rank < len(producers):
            # producers whose feed can't be separated from another's (e.g. both sell only to the same consumer)
            _, _, vt = np.linalg.svd(Rp)
            unclear = codes[producers[np.abs(vt[rank:]).max(axis=0) > 1e-8]]
            print(f"Warning: item {int(item)}: feed per tonne isn't uniquely determined for producers {unclear.tolist()}")
    g[g < 0] = 0  # float noise around zero
    return g, codes[producers]


def feed_per_tonne_produced(trade: pd.DataFrame, feed: pd.DataFrame, direct: pd.DataFrame) -> pd.DataFrame:
    """Feed tonnes and feed impacts per tonne of each animal product, by the country
    that produced it.

    TradeMatrixFeed holds each consuming country's feed footprint: for animal
    product a and consumer c, F_c = sum_p R[c, p] * r_p, where R is the MRIO trade
    matrix for a and r_p is the feed (by crop and origin) per tonne of a produced
    in p. Each feed row is priced with the direct per-kg impacts of that crop where
    it was grown (falling back to the crop's world average), giving the feed
    impacts G_c embodied in c's consumption of a. Solving R g = G then recovers g_p,
    the feed impacts per tonne of a produced in p. The same solve on the errors
    sums them linearly, as elsewhere.
    """

    # per-kg direct impacts of each crop where it was grown, falling back to the
    # crop's world average (and for the odd undefined, inf, error)
    usable = direct[direct["provenance_tonnes"] > 0]
    per_kg = usable[IMPACT_AND_ERR_COLS].div(usable["provenance_tonnes"] * 1000, axis=0)
    per_kg = per_kg.replace([np.inf, -np.inf], np.nan)
    per_kg[["Item_Code", "Producer_Country_Code"]] = usable[["Item_Code", "Producer_Country_Code"]]

    feed = feed.merge(per_kg, on=["Item_Code", "Producer_Country_Code"], how="left")
    for col in IMPACT_AND_ERR_COLS:
        finite = usable[np.isfinite(usable[col])].groupby("Item_Code")
        world = finite[col].sum() / (finite["provenance_tonnes"].sum() * 1000)
        feed[col] = feed[col].fillna(feed["Item_Code"].map(world))
    unpriced = feed[IMPACT_AND_ERR_COLS[0]].isna()
    if unpriced.any():
        print(f"Warning: {feed.loc[unpriced, 'Value'].sum() / feed['Value'].sum():.2%} of feed tonnes are crops "
              f"with no impacts anywhere ({sorted(feed.loc[unpriced, 'Item_Code'].unique().astype(int))}) and are left unpriced")
    feed[IMPACT_AND_ERR_COLS] = feed[IMPACT_AND_ERR_COLS].mul(feed["Value"] * 1000, axis=0).fillna(0)
    feed = feed.rename(columns={"Value": "feed_tonnes"})
    rhs_cols = ["feed_tonnes"] + IMPACT_AND_ERR_COLS
    footprints = feed.groupby(["Animal_Product_Code", "Consumer_Country_Code"])[rhs_cols].sum()

    out = []
    for item, fp in footprints.groupby(level=0):
        g, producers = solve_per_producer(trade, item, fp.droplevel(0))
        g = pd.DataFrame(g, columns=rhs_cols)
        g["ItemT_Code"] = int(item)
        g["Effective_Producer_Code"] = producers
        out.append(g)

    return pd.concat(out, ignore_index=True)


def feed_mix_per_kg_produced(trade: pd.DataFrame, feed: pd.DataFrame) -> pd.DataFrame:
    """Feed by crop and where it was grown, in kg per kg of each animal product, by
    the country that produced it: the solve of feed_per_tonne_produced on the feed
    tonnes of each crop x origin rather than on their impacts."""

    out = []
    for item, f in tqdm(feed.groupby("Animal_Product_Code"), desc="Solving feed mixes"):
        footprints = f.pivot_table(index="Consumer_Country_Code", columns=["Item_Code", "Producer_Country_Code"],
                                   values="Value", aggfunc="sum", fill_value=0)
        # the same trade matrices as feed_per_tonne_produced, which warns about them
        g, producers = solve_per_producer(trade, item, footprints, warn=False)
        p, s = np.nonzero(g > MIN_FEED_KG_PER_KG)
        out.append(pd.DataFrame({
            "ItemT_Code": int(item),
            "Effective_Producer_Code": producers[p],
            "Feed_Item_Code": footprints.columns.get_level_values(0)[s],
            "Feed_Origin_Code": footprints.columns.get_level_values(1)[s],
            "feed_kg_per_kg": g[p, s],
        }))
    return pd.concat(out, ignore_index=True)


def build_prod_impacts_long(results_dir: Path, year: int, trade: pd.DataFrame,
                            feed: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Impacts and production tonnage by (item, producing country): the direct part,
    plus the feed part for animal products."""

    direct, item_names = build_direct_long(results_dir, year)
    feed = feed_per_tonne_produced(trade, feed, direct)

    long_df = direct.rename(columns={"Item_Code": "ItemT_Code", "Producer_Country_Code": "Effective_Producer_Code",
                                     "provenance_tonnes": "production_tonnes"})
    long_df = long_df.merge(feed, on=["ItemT_Code", "Effective_Producer_Code"], how="left", suffixes=("", "_per_t_feed"))
    for col in IMPACT_COLS:
        for suffix in ("", "_err"):
            total = f"{col}{suffix}"
            long_df[f"{col}_direct{suffix}"] = long_df[total]
            long_df[f"{col}_feed{suffix}"] = (long_df[f"{total}_per_t_feed"] * long_df["production_tonnes"]).fillna(0)
            long_df[total] = long_df[f"{col}_direct{suffix}"] + long_df[f"{col}_feed{suffix}"]
    long_df["feed_tonnes"] = (long_df["feed_tonnes"] * long_df["production_tonnes"]).fillna(0)

    return long_df[["ItemT_Code", "Effective_Producer_Code"] + VALUE_COLS], item_names


def add_labels(long_df: pd.DataFrame, item_names: dict, input_data_dir: Path) -> pd.DataFrame:
    """Attach the producer ISO3, the item name and the commodity group."""

    long_df = long_df.copy()
    long_df["ItemT_Name"] = long_df["ItemT_Code"].map(item_names)
    long_df["Country"] = long_df["Effective_Producer_Code"].map(_leakage_funcs.load_country_isos(input_data_dir))

    commodity_crosswalk = pd.read_csv(input_data_dir / "commodity_crosswalk.csv")
    long_df = long_df.merge(
        commodity_crosswalk[["Item_Code", GROUPING]],
        left_on="ItemT_Code",
        right_on="Item_Code",
        how="left",
    ).drop(columns=["Item_Code"])

    return long_df


def impact_per_kg(bd: pd.Series, tonnes: pd.Series) -> pd.Series:
    """Extinctions per kg, NaN (not inf) where no production was traced."""
    kg = tonnes * 1000
    return bd.where(kg > 0) / kg.where(kg > 0)


def mask_undefined_errors(err: pd.Series, label: str) -> pd.Series:
    """A handful of upstream rows carry bd_opp_cost_calc_err = inf while their
    bd_opp_cost_calc is finite (7 rows, in BDI and TCD, for 2021/spam2020).
    Summing errors linearly propagates that to every cell those rows touch, so
    write those cells as NaN - error unknown - rather than inf."""
    undefined = np.isinf(err)
    if undefined.any():
        print(
            f"Warning: {undefined.sum()} {label} cells have an undefined error "
            f"(inf error upstream) and are written as NaN"
        )
    return err.mask(undefined)


def build_matrices(long_df: pd.DataFrame, level: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pivot to Country x <level>, summing before dividing so the group-level
    intensity is production-weighted rather than a mean of per-item ratios."""

    df = long_df.dropna(subset=["Country", level])
    df = df.groupby(["Country", level], as_index=False)[VALUE_COLS].sum()

    df["impact_per_kg"] = impact_per_kg(df["life_extinctions_per_sp_calc"], df["production_tonnes"])
    df["impact_per_kg_err"] = mask_undefined_errors(
        impact_per_kg(df["life_extinctions_per_sp_calc_err"], df["production_tonnes"]), level
    )

    matrix = df.pivot(index="Country", columns=level, values="impact_per_kg")
    err_matrix = df.pivot(index="Country", columns=level, values="impact_per_kg_err")

    return matrix, err_matrix


def add_per_kg_columns(long_df: pd.DataFrame) -> pd.DataFrame:
    """Per kg of production for every impact: total, direct and feed parts, and
    the error of the total (direct and feed errors summed linearly)."""

    long_df = long_df.copy()
    tonnes = long_df["production_tonnes"]
    for col in IMPACT_COLS:
        long_df[f"{col}_per_kg"] = impact_per_kg(long_df[col], tonnes)
        long_df[f"{col}_per_kg_err"] = mask_undefined_errors(
            impact_per_kg(long_df[f"{col}_err"], tonnes), f"{col} long-format"
        )
        for part in ("direct", "feed"):
            long_df[f"{col}_{part}_per_kg"] = impact_per_kg(long_df[f"{col}_{part}"], tonnes)

    return long_df


def main(results_dir: Path, year: int, input_data_dir: Path, out_dir: Path) -> Path:
    """Build every matrix and the prod_impacts_long and feed_mix_long tables for
    one year from the FLMP results_dir into out_dir, and return the
    prod_impacts_long path."""

    results_dir, input_data_dir, out_dir = Path(results_dir), Path(input_data_dir), Path(out_dir)

    trade, feed = read_mrio(results_dir, year)
    long_df, item_names = build_prod_impacts_long(results_dir, year, trade, feed)
    long_df = add_labels(long_df, item_names, input_data_dir)

    long_df["impact_per_kg"] = impact_per_kg(long_df["life_extinctions_per_sp_calc"], long_df["production_tonnes"])
    long_df["impact_per_kg_err"] = mask_undefined_errors(
        impact_per_kg(long_df["life_extinctions_per_sp_calc_err"], long_df["production_tonnes"]),
        "long-format",
    )

    item_matrix, item_err_matrix = build_matrices(long_df, "ItemT_Name")
    group_matrix, group_err_matrix = build_matrices(long_df, GROUPING)

    out_dir.mkdir(exist_ok=True, parents=True)
    out_path = out_dir / f"prod_impacts_long_{year}.csv"

    item_matrix.to_csv(out_dir / f"bd_prod_extinctions_per_kg_item_{year}.csv")
    item_err_matrix.to_csv(out_dir / f"bd_prod_extinctions_per_kg_item_err_{year}.csv")
    group_matrix.to_csv(out_dir / f"bd_prod_extinctions_per_kg_group_{year}.csv")
    group_err_matrix.to_csv(out_dir / f"bd_prod_extinctions_per_kg_group_err_{year}.csv")

    long_df[
        [
            "Country",
            "Effective_Producer_Code",
            "ItemT_Code",
            "ItemT_Name",
            GROUPING,
            "life_extinctions_per_sp_calc",
            "life_extinctions_per_sp_calc_err",
            "production_tonnes",
            "impact_per_kg",
            "impact_per_kg_err",
        ]
    ].to_csv(out_dir / f"bd_prod_extinctions_long_{year}.csv", index=False)

    print(f"Wrote {item_matrix.shape[0]} x {item_matrix.shape[1]} item matrices to {out_dir}")
    print(f"Wrote {group_matrix.shape[0]} x {group_matrix.shape[1]} group matrices to {out_dir}")

    # all impacts, per kg of production, direct and feed parts kept separate
    all_impacts = add_per_kg_columns(long_df)
    all_impacts["feed_kg_per_kg"] = all_impacts["feed_tonnes"] / all_impacts["production_tonnes"].where(lambda t: t > 0)
    per_kg_cols = [
        c for col in IMPACT_COLS
        for c in (f"{col}_per_kg", f"{col}_per_kg_err", f"{col}_direct_per_kg", f"{col}_feed_per_kg")
    ]
    all_impacts[
        ["Country", "Effective_Producer_Code", "ItemT_Code", "ItemT_Name", GROUPING, "production_tonnes", "feed_tonnes"]
        + IMPACT_AND_ERR_COLS
        + SPLIT_COLS
        + ["feed_kg_per_kg"]
        + per_kg_cols
    ].to_csv(out_path, index=False)

    labelled = all_impacts.dropna(subset=["Country", "ItemT_Name"])
    for col in IMPACT_COLS:
        for suffix in ("", "_err"):
            labelled.pivot(index="Country", columns="ItemT_Name", values=f"{col}_per_kg{suffix}").to_csv(
                out_dir / f"prod_{col}_per_kg_item{suffix}_{year}.csv"
            )

    print(f"Wrote prod_impacts_long_{year}.csv and {2 * len(IMPACT_COLS)} per-impact item matrices to {out_dir}")

    feed_mix_per_kg_produced(trade, feed).to_csv(out_dir / f"feed_mix_long_{year}.csv", index=False)
    print(f"Wrote feed_mix_long_{year}.csv to {out_dir}")

    return out_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=YEAR)
    parser.add_argument("--results-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument("--input-data-dir", type=Path, default=INPUT_DATA_DIR)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    main(args.results_dir, args.year, args.input_data_dir, args.out_dir)
