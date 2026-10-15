Market leakage of displaced agricultural production: when a country produces less of an item, how much land, biodiversity and GHG impact moves to the countries that replace it.

## Inputs

Set these at the top of `run_countries.py`:

1. `FLMP_RESULTS_DIR`: a results dir from [FLMP](https://github.com/louis-de-neve/mrio_pipeline), run for the year you want. The trade matrix `{year}/.mrio/TradeMatrix_import_dry_matter.csv` is read from it.
2. `IMPACTS_PATH`: per-kg production impacts by producing country and item, by default `{FLMP_RESULTS_DIR}/impacts_matrices/prod_impacts_long_{year}.csv`. If it's missing, `run_countries.py` builds it first with `_build_bd_prod_impacts_matrix.py` (from every `{year}/*/impacts_full.csv` in the results dir). To build it on its own, run `python _build_bd_prod_impacts_matrix.py --year {year} --results-dir {FLMP_RESULTS_DIR}`.
3. `data/`: `country_codes.csv` and `commodity_crosswalk.csv`, already in this repo.

Then run `python run_countries.py`. All countries and items for one year take under a minute.

## Method

For each item, with FLMP's MRIO result R (consumers × producers):

- E = R / column sums: where each producer's output goes.
- S = R / row sums: where each consumer gets its supply.
- L = EᵀS: L[C, p] is the share of C's displaced production replaced by producer p.

The leakage per kg displaced in C is Σ_p L[C, p]·I[p] − I[C], where I is the per-kg production impact. For animal products, I is the pasture at the producer plus the feed it uses per kg produced, each feed crop priced where it was grown (recovered from FLMP's feed footprints by the matrix script).

Options:
- `COMPREHENSIVE = False`: only C's own consumers replace the production (L[C] = S[C]).
- `EXCLUDE_SELF = True`: C can't replace its own displaced production. C is dropped from each consumer's supply mix and the rest renormalised. Consumers that only bought from C turn to the world production mix.
- With the default `EXCLUDE_SELF = False`, part of the replacement comes from C itself.
- `EXTENSIFICATION_VALUE`: the extensification factor passed, with each replacing producer's current production and the kg arriving there, to `leakage_land_use` in `_leakage_funcs.py`, which gives the land use of the leaked production. It's a float for every producer and item, with `EXTENSIFICATION_STD` as its standard deviation, or `"beta"` for the per country × item factors in `data/beta_crops.csv` and `data/beta_animals.csv` (downloaded by `_get_extensification_data.py`).

## Outputs (`results_leakage/{year}/`)

- `{ISO}_leakage_per_kg.csv`: per kg displaced. There is one `replacement` row per replacing producer and one `displaced` row with C's own impacts negated.
  - Columns: arable and pasture land (m², always separate; pasture is 0 for crops, and arable for animal products is feed land), extinctions (`bd`), production GHG and one-off carbon opportunity cost (kg CO2e), the last three with errors summed linearly.
  - `fallback_intensity` marks producers with no FLMP value, which get the item's production-weighted world average.
  - `extensification` and `extensification_std` are the factor used for each replacing producer, and `land_use_leakage_m2` is the land use from `leakage_land_use` (all NaN on the `displaced` row).
- `{ISO}_leakage.csv`: the same, per m² of land spared in C (its arable + pasture per kg).
- `leakage_summary_per_kg.csv`: per country × item, the impacts spared at home, leaked elsewhere and their ratio.
- File names get `NOEXP_` / `NOSELF_` prefixes for the non-default options, and an `EXT{EXTENSIFICATION_VALUE}_` prefix.
