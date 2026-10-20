Market leakage of displaced agricultural production: when a country produces less of an item, how much land, biodiversity and GHG impact moves to the countries that replace it.

## Inputs

Set these at the top of `run_countries.py`:

1. `FLMP_RESULTS_DIR`: a results dir from [FLMP](https://github.com/louis-de-neve/mrio_pipeline), run for the year you want. It's only read from: the trade matrix `{year}/.mrio/TradeMatrix_import_dry_matter.csv`, and for the build below the feed matrix and every `{year}/*/impacts_full.csv`.
2. `IMPACTS_PATH` and `FEED_MIX_PATH`: per-kg production impacts by producing country and item, and the feed by crop and origin per kg of each animal product by producing country, by default `{RESULTS_DIR}/impacts_matrices/prod_impacts_long_{year}.csv` and `feed_mix_long_{year}.csv`. If either is missing, `run_countries.py` builds them first with `_build_bd_prod_impacts_matrix.py`. To build them on their own, run `python _build_bd_prod_impacts_matrix.py --year {year} --results-dir {FLMP_RESULTS_DIR} --out-dir {RESULTS_DIR}/impacts_matrices`.
3. `data/`: `country_codes.csv` and `commodity_crosswalk.csv`, already in this repo, and `beta_crops.csv` and `beta_animals.csv`, downloaded by `python _get_extensification_data.py`.

Then run `python run_countries.py`. All countries and items for one year take under a minute.

## Method

For each item, with FLMP's MRIO result R (consumers × producers):

- E = R / column sums: where each producer's output goes.
- S = R / row sums: where each consumer gets its supply.
- L = EᵀS: L[C, p] is the share of C's displaced production replaced by producer p.

The leakage per kg displaced in C is Σ_p L[C, p]·I[p] − I[C], where I is the per-kg production impact. For animal products, I is the pasture at the producer plus the feed it uses per kg produced, each feed crop priced where it was grown (recovered from FLMP's feed footprints by the matrix script).

Extensification: the same leakage is also given with land use, and the impacts that follow it (extinctions and carbon opportunity cost), changing less (or more) than production. The change in each land source goes through `leakage_land_use` in `_leakage_funcs.py` with that source's own extensification factor β, current production and land use. The sources are a crop's own land, an animal product's pasture, and each of its feed crops where they were grown. This applies to C's displaced production (−1 kg) and to each producer's replacement share alike. Each source's impacts are scaled by its extensified over its linear land change, which is ≈ β for the kg sizes here. Production GHG isn't scaled.

Options:
- `COMPREHENSIVE = False`: only C's own consumers replace the production (L[C] = S[C]).
- `EXCLUDE_SELF = True`: C can't replace its own displaced production. C is dropped from each consumer's supply mix and the rest renormalised. Consumers that only bought from C turn to the world production mix.
- With the default `EXCLUDE_SELF = False`, part of the replacement comes from C itself.
- `EXTENSIFICATION_VALUE`: the extensification factor passed, with each replacing producer's current production and land use and the kg arriving there, to `leakage_land_use` in `_leakage_funcs.py`, which gives the land use of the leaked production. It's a float for every producer and item, with `EXTENSIFICATION_STD` as its standard deviation, or `"beta"` for the per country × item factors (`beta_{year}`, `se_{year}`) in `data/beta_crops.csv` and `data/beta_animals.csv`, with negative betas set to 0. Current production and land use always come from those files' `current_*` columns. A producer with no factor, or no (or zero) production or land use, gets the item's World row instead. Animal items (`PASTURE_ITEMS` in `_leakage_funcs.py`) take the per-country "All pasture-based animal products" row of `beta_animals.csv`, with the land use set to the item's own FLMP pasture per kg times that production, so feed-fed animals (zero pasture) only extensify through their feed.

## Outputs (`results_leakage/{year}/`)

- `{ISO}_leakage_per_kg.csv`: per kg displaced. There is one `replacement` row per replacing producer and one `displaced` row with C's own impacts negated.
  - Columns: arable and pasture land (m², always separate; pasture is 0 for crops, and arable for animal products is feed land), extinctions (`bd`), production GHG and one-off carbon opportunity cost (kg CO2e), the last three with errors summed linearly.
  - `fallback_intensity` marks producers with no FLMP value, which get the item's production-weighted world average.
  - `extensification` and `extensification_std` are the producer's factor (for animal products, its pasture's). `land_use_leakage_m2`, `bd_ext` and `ghg_coc_kgco2e_ext` are the extensified land use and impacts, on both the `displaced` and `replacement` rows. `fallback_extensification` marks producers that got the World row for any of their own inputs.
- `{ISO}_leakage.csv`: the same, per m² of land spared in C (its arable + pasture per kg).
- `leakage_summary_per_kg.csv`: per country × item, the impacts spared at home, leaked elsewhere and their ratio, extensified ones included.
- File names get `NOEXP_` / `NOSELF_` prefixes for the non-default options, and an `EXT{EXTENSIFICATION_VALUE}_` prefix.
