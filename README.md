Calcultes market leakage of displaced agricultural production: when a country produces less of an item, how much land, biodiversity and GHG impact moves to the countries that replace it. Uses 'extensification factors' - elasticities between production and farmed area for each commodity, to estimate the extent to which foregone production motivates the clearing of new agricultural land (and consequently has high impacts in LIFE and carbon, etc) in the places that production is displaced to. 
- Thomas S Ball, 20-10-2026

## Inputs

1. An FLMP results dir (`paths.flmp_results_dir`), from [FLMP](https://github.com/louis-de-neve/mrio_pipeline) run for the year you want. It's only read from: the trade matrix `{year}/.mrio/TradeMatrix_import_dry_matter.csv`, and for the build below the feed matrix and every `{year}/*/impacts_full.csv`.
2. `prod_impacts_long_{year}.csv` and `feed_mix_long_{year}.csv` in `paths.impacts_dir`: per-kg production impacts by producing country and item, and the feed by crop and origin per kg of each animal product by producing country. If either is missing, `run_countries.py` builds them first with `_build_bd_prod_impacts_matrix.py`. To build them on their own, run `python _build_bd_prod_impacts_matrix.py --year {year} --results-dir {flmp_results_dir} --out-dir {impacts_dir}`.
3. `data/`: `country_codes.csv` and `commodity_crosswalk.csv`, already in this repo, and `beta_crops.csv` and `beta_animals.csv`, downloaded by `python _get_extensification_data.py`.

Then run `python run_countries.py`, which uses the repository's `config.json`, or `python run_countries.py path/to/config.json`. All countries and items for one year take about a minute, plus a minute and a half the first time to build the impacts matrices.

## Configuration (`config.json`)

| Setting | |
|---|---|
| `run.run_name` | The run's folder name, under `paths.results_dir`. |
| `run.year` | The year of the FLMP results and of the betas (`beta_{year}`, `se_{year}`). |
| `run.countries` | ISO3 codes of the countries of interest, e.g. `["GBR"]`, or `null` for every country. |
| `paths.flmp_results_dir` | The FLMP results dir. |
| `paths.results_dir` | Where the run folders go. |
| `paths.impacts_dir` | Where `prod_impacts_long` and `feed_mix_long` are read from, and built if missing. `null` for `{results_dir}/{run_name}/impacts_matrices`; point several runs on the same FLMP results at one folder to build them only once. |
| `paths.data_dir` | The folder with the files in Inputs 3. |
| `replacement.comprehensive` | See Options. |
| `replacement.exclude_self` | See Options. |
| `extensification.value` | The extensification factor: a number for every producer and item, or `"beta"`. See Options. |
| `extensification.std` | Its standard deviation, when `value` is a number (the betas carry their own). |
| `extensification.clipping` | `[lower, upper]` bounds the betas are clipped to, `null` for no bound, e.g. `[0, null]` clips at 0 but not above; or `null` for no clipping. A numeric `value` isn't clipped. |

Relative paths are from the repository folder.

## Recreating a run

Each run writes its record, `config.json`, into its folder when it finishes. That is its config plus a `provenance` section with:
- when it finished and the command run;
- the repository's remote, branch and commit, and whether the commit is on the remote;
- any uncommitted changes, as one patch on that commit, with untracked files up to 1 MB included as new files (larger ones are only listed);
- the Python and package versions;
- the md5 of every input file read.

To recreate a run:
1. `git checkout` the recorded commit, and `git apply` the uncommitted changes if there are any.
2. Change `run.run_name` (or `paths.results_dir`) in a copy of the record, so as not to overwrite the original.
3. Run `python run_countries.py <that copy>`. It warns if the repository isn't at the recorded commit.

Comparing the input md5s with the files you have shows whether the inputs match. A run folder can't be reused for different settings.

## Method

For each item, with FLMP's MRIO result R (consumers × producers):

- E = R / column sums: where each producer's output goes.
- S = R / row sums: where each consumer gets its supply.
- L = EᵀS: L[C, p] is the share of C's displaced production replaced by producer p.

The leakage per kg displaced in C is Σ_p L[C, p]·I[p] − I[C], where I is the per-kg production impact. For animal products, I is the pasture at the producer plus the feed it uses per kg produced, each feed crop priced where it was grown (recovered from FLMP's feed footprints by the matrix script).

Extensification: the same leakage is also given with land use, and the impacts that follow it (extinctions and carbon opportunity cost), changing less (or more) than production. The change in each land source goes through `leakage_land_use` in `_leakage_funcs.py` with that source's own extensification factor β, current production and land use. The sources are a crop's own land, an animal product's pasture, and each of its feed crops where they were grown. This applies to C's displaced production (−1 kg) and to each producer's replacement share alike. Each source's impacts are scaled by its extensified over its linear land change, which is ≈ β for the kg sizes here. Production GHG isn't scaled.

Options:
- `replacement.comprehensive: false`: only C's own consumers replace the production (L[C] = S[C]).
- `replacement.exclude_self: true`: C can't replace its own displaced production. C is dropped from each consumer's supply mix and the rest renormalised. Consumers that only bought from C turn to the world production mix.
- With the default `replacement.exclude_self: false`, part of the replacement comes from C itself.
- `extensification.value`: the extensification factor passed, with each replacing producer's current production and land use and the kg arriving there, to `leakage_land_use` in `_leakage_funcs.py`, which gives the land use of the leaked production. It's a number for every producer and item, with `extensification.std` as its standard deviation, or `"beta"` for the per country × item factors (`beta_{year}`, `se_{year}`) in `data/beta_crops.csv` and `data/beta_animals.csv`, clipped to `extensification.clipping` (the default `[0, null]` sets negative betas to 0). Current production and land use always come from those files' `current_*` columns. A producer with no factor, or no (or zero) production or land use, gets the item's World row instead. Animal items (`PASTURE_ITEMS` in `_leakage_funcs.py`) take the per-country "All pasture-based animal products" row of `beta_animals.csv`, with the land use set to the item's own FLMP pasture per kg times that production, so feed-fed animals (zero pasture) only extensify through their feed.

## Outputs (`{results_dir}/{run_name}/`)

- `{ISO}_leakage_per_kg.csv`: per kg displaced. There is one `replacement` row per replacing producer and one `displaced` row with C's own impacts negated.
  - Columns: arable and pasture land (m², always separate; pasture is 0 for crops, and arable for animal products is feed land), extinctions (`bd`), production GHG and one-off carbon opportunity cost (kg CO2e), the last three with errors summed linearly.
  - `fallback_intensity` marks producers with no FLMP value, which get the item's production-weighted world average.
  - `extensification` and `extensification_std` are the producer's factor (for animal products, its pasture's). `land_use_leakage_m2`, `bd_ext` and `ghg_coc_kgco2e_ext` are the extensified land use and impacts, on both the `displaced` and `replacement` rows. `fallback_extensification` marks producers that got the World row for any of their own inputs.
- `{ISO}_leakage.csv`: the same, per m² of land spared in C (its arable + pasture per kg).
- `leakage_summary_per_kg.csv`: per country × item, the impacts spared at home, leaked elsewhere and their ratio, extensified ones included.
- File names get `NOEXP_` / `NOSELF_` prefixes for the non-default options, and an `EXT{extensification.value}_` prefix.
- `config.json`: the run's record (see Recreating a run).
- `impacts_matrices/`: the matrix script's outputs, when `paths.impacts_dir` is `null`.
