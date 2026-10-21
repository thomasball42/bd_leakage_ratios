Calcultes market leakage of displaced agricultural production: when a country produces less of an item, how much land, biodiversity and GHG impact moves to the countries that replace it. Uses 'extensification factors' - elasticities between production and farmed area for each commodity, to estimate the extent to which foregone production motivates the clearing of new agricultural land (and consequently has high impacts in LIFE and carbon, etc) in the places that production is displaced to. 
- Thomas S Ball, 20-10-2026

## Inputs

1. An FLMP results dir (`paths.flmp_results_dir`), from [FLMP](https://github.com/louis-de-neve/mrio_pipeline) run for the year you want. It's only read from: the trade matrix `{year}/.mrio/TradeMatrix_import_dry_matter.csv`, and for the build below the feed matrix and every `{year}/*/impacts_full.csv`.
2. `prod_impacts_long_{year}.csv` and `feed_mix_long_{year}.csv` in `paths.impacts_dir`: per-kg production impacts and production by producing country and item, and the feed by crop and origin per kg of each animal product by producing country. If either is missing, `run_countries.py` builds them first with `_build_bd_prod_impacts_matrix.py`. To build them on their own, run `python _build_bd_prod_impacts_matrix.py --year {year} --results-dir {flmp_results_dir} --out-dir {impacts_dir}`.
3. `data/`: `country_codes.csv` and `commodity_crosswalk.csv`, already in this repo, and `beta_crops.csv` and `beta_animals.csv`, downloaded by `python _get_extensification_data.py`. Only their `beta_{year}` and `se_{year}` columns are used, for the FLMP year.

Then run `python run_countries.py`, which uses the repository's `config.json`, or `python run_countries.py path/to/config.json`. Add `--workers N` to run the items in N processes at once (see Running in parallel).

For all countries and items, each displacement size takes about 6 minutes with 1,000 Monte Carlo draws, or about 1.5 minutes with 100. Building the impacts matrices takes another minute and a half the first time. Peak memory is about 2 GB at 1,000 draws, and each size writes about 0.5 GB of results.

## Running in parallel

`python run_countries.py --workers N` computes the items in N processes at once, and writes the results files in them too. They use the same Monte Carlo draws, and each size's files are all written, in the same order, before the next size's, so the results are the same as a serial run's. (The Monte Carlo columns can differ in float32 rounding, by up to about 1e-5 of the spared and leaked amounts, as a serial run's matrix products use several BLAS threads and each worker's one; with `OPENBLAS_NUM_THREADS=1` the serial run's files are byte-for-byte the same.) As the number of workers doesn't change the results, it isn't a setting in `config.json`, but the record's `command` shows it.

With all countries and 1,000 draws, a size takes about 36 seconds with 16 workers, or 27 with 32, against about 6 minutes serially, so 50 sizes take 20-30 minutes rather than 5 hours. More than about 32 workers won't help, as the slowest items (animal products, through their feed, at about 50 seconds each) then set the pace. 32 workers need about 23 GB of memory in all.

The workers are forked, which needs Linux or macOS, and use one BLAS thread each unless `OPENBLAS_NUM_THREADS`, `OMP_NUM_THREADS` or `MKL_NUM_THREADS` is already set.

## Configuration (`config.json`)

| Setting | |
|---|---|
| `run.run_name` | The run's folder name, under `paths.results_dir`. |
| `run.year` | The year of the FLMP results and of the betas (`beta_{year}`, `se_{year}`). Everything uses this one year. |
| `run.countries` | ISO3 codes of the countries of interest, e.g. `["GBR"]`, or `null` for every country. |
| `paths.flmp_results_dir` | The FLMP results dir. |
| `paths.results_dir` | Where the run folders go. |
| `paths.impacts_dir` | Where `prod_impacts_long` and `feed_mix_long` are read from, and built if missing. `null` for `{results_dir}/{run_name}/inputs/impacts_matrices`; point several runs on the same FLMP results at one folder to build them only once. |
| `paths.data_dir` | The folder with the files in Inputs 3. |
| `displacement.unit` | `"kg"`, or `"share"` for a share of each country of interest's FLMP production of the item. |
| `displacement.sizes` | The displacement sizes to run, in that unit, e.g. `[1]` kg or `[0.001, 0.01, 0.1, 0.5]` of production. Or `[]` to generate them from `sizes_range`, `sizes_dist` and `sizes_n`. Every size uses the same Monte Carlo draws. |
| `displacement.sizes_range` | When `sizes` is `[]`: `[lower, upper]` bounds of the generated sizes, both included, in that unit, e.g. `[0.001, 1]` of production or `[1, 100000000]` kg. |
| `displacement.sizes_dist` | When `sizes` is `[]`: `"geometric"` (evenly spaced on a log scale) or `"arithmetic"` (evenly spaced) sizes between those bounds. |
| `displacement.sizes_n` | When `sizes` is `[]`: the number of sizes to generate. |
| `replacement.comprehensive` | See Options. |
| `replacement.exclude_self` | See Options. |
| `extensification.use_beta` | `true` to use the betas in `data/beta_*.csv` (`value` and `std` are then ignored), or `false` to use `value` and `std` in place of every beta and se there. See Options. |
| `extensification.value` | The extensification factor used in place of every beta, when `use_beta` is `false`. |
| `extensification.std` | The standard deviation used in place of every beta's se, when `use_beta` is `false`. |
| `extensification.clipping` | `[lower, upper]` bounds the betas are clipped to, central and drawn, with `null` for no bound, e.g. `[0, null]` clips at 0 but not above; or `null` for no clipping. |
| `monte_carlo.draws` | The number of Monte Carlo draws of the betas, or 0 for central values only. |
| `monte_carlo.seed` | The random seed for the draws. |
| `monte_carlo.percentiles` | The percentiles of the draws to report, e.g. `[5, 50, 95]`. |
| `outputs.per_kg` | Also write the per-kg files (see Outputs). |
| `outputs.per_m2` | Also write the per m² of spared land files. |

Relative paths are from the repository folder.

## Recreating a run

Each run writes its record, `config.json`, into its folder when it finishes. That is its config plus a `provenance` section with:
- when it finished and the command run;
- the repository's remote, branch and commit, and whether the commit is on the remote;
- any uncommitted changes, as one patch on that commit, with untracked files up to 1 MB included as new files (larger ones are only listed);
- the Python and package versions;
- the md5 of every input file read.

The record includes the Monte Carlo seed, so a rerun gives identical draws.

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

When C displaces m kg (a displacement size), producer p replaces L[C, p]·m kg.

**Extensification.** Land use, and the impacts that follow it (extinctions and carbon opportunity cost), change less (or more) than production. The change at each land source goes through `leakage_land_use` in `_leakage_funcs.py`, A·[((Q + Δ)/Q)^β − 1], with:
- that source's beta β;
- its FLMP production Q for the run year (`production_tonnes` in `prod_impacts_long`, or the trade matrix's where that has none);
- its FLMP land A, its FLMP land per kg times Q.

The land sources are:
- a crop's own arable land;
- an animal product's pasture: its own pasture per kg, on the scale of its producer's FLMP production of all its pasture-based products, as its pasture beta is defined that way;
- each of an animal product's feed crops where they were grown, with feed by crop and origin from `feed_mix_long` (recovered from FLMP's feed footprints by the matrix script).

Within each country of interest's scenario, the changes at each source are netted before the curve is applied: C's loss against what C replaces itself, and feed origins shared between producers. Each of C's loss and the replacing production then gets its pro rata part. Each source's extinctions and carbon opportunity cost are scaled by its extensified over its linear land change, and production GHG isn't scaled. Because A/Q is FLMP's own land per kg, at β = 1 the extensified land equals FLMP's linear land.

**Location.** The land-based results (land use, extinctions and carbon opportunity cost) are reported where the land is: a crop's where it's grown, an animal product's pasture where the animal is, and its feed's land where the feed was grown. So when Irish beef replaces displaced British beef, the land for the extra Brazilian soy it's fed counts in Brazil, and the British beef's own Brazilian soy is spared there too. Production GHG is reported where the item is produced.

Because the curve isn't linear, the results depend on the displacement size relative to each source's production. At small sizes they're the marginal values: β times each source's land per kg. For β < 1, a larger displacement frees more land per kg at home and needs less per kg elsewhere, so leaked ÷ spared falls. A country × item where a source would lose more than it produces has no extensified results.

**Monte Carlo.** Each draw takes every beta estimate (each row of `data/beta_*.csv`, World rows included) from Normal(β, se), independently, and clips it if `clipping` is set. A beta gets the same draw everywhere it's used in that draw: on both sides of a scenario, in every scenario, and as a feed origin. Producers with no beta of their own share the World row's. The results are reported at the central betas, with percentiles and the standard deviation over the draws. FLMP's own errors aren't drawn: they're carried separately, scaled by the central extensification, for combining downstream.

Options:
- `replacement.comprehensive: false`: only C's own consumers replace the production (L[C] = S[C]).
- `replacement.exclude_self: true`: C can't replace its own displaced production. C is dropped from each consumer's supply mix and the rest renormalised. Consumers that only bought from C turn to the world production mix.
- With the default `replacement.exclude_self: false`, part of the replacement comes from C itself.
- `extensification.use_beta`:
  - `true` uses each country × item's `beta_{year}` and `se_{year}`. A producer with no beta of its own gets the item's World row. So does one whose row is the filter's unfitted prior (β 0.5, se 1000, where it had no data), as any row with an se above `MAX_BETA_SE` (100) is treated as missing.
  - `false` replaces every beta in those files with `extensification.value`, and every se with `extensification.std`, so each country × item still gets its own draw.
  - Animal items (`PASTURE_ITEMS` in `_leakage_funcs.py`) take their country's "All pasture-based animal products" row of `beta_animals.csv`. Where they have no pasture (pigs, poultry, eggs), they have no pasture beta and extensify only through their feed.

## Outputs (`{results_dir}/{run_name}/raw_outputs/`)

The results files go in the run folder's `raw_outputs/`, any inputs built for the run in its `inputs/`, and the run's record (`config.json`) in the run folder itself. Every file has a `size` column, and has rows for every displacement size.

- `{ISO}_leakage.csv` (the main results): one row per location (`Location_ISO`, `Location_Country_Code`) for each flow:
  - `displaced` rows for each location where C's lost production had its land: C, and for animal products the origins of C's feed;
  - `replacement` rows for each location where the replacing production has its land or is produced.
  - **Driving values:** `displaced_kg` (m) and `production_change_kg`, the location's change in production of the item, as a producer: L[C, location]·m in its replacement row, −m in C's displaced row, and 0 where it only grows feed.
  - **Extensified results located there:**
    - `land_use_m2`, `bd` (extinctions) and `ghg_coc_kgco2e` (one-off carbon opportunity cost), each with `_p{q}` percentiles and `_sd` over the draws;
    - `bd_err` and `ghg_coc_kgco2e_err`, FLMP's own errors scaled the same way;
    - `ghg_prod_kgco2e` and `_err`, production GHG of the location's `production_change_kg`, which isn't extensified.
  - **Betas,** of the location as a producer of the item: `extensification` and `extensification_std` are its central beta and se. For animal products these are its pasture's, which are NaN where it has no pasture. `fallback_extensification` marks producers using the World row's beta, and `fallback_intensity` producers with no FLMP impacts, which get the item's production-weighted world average.
  - **Current state** of the location as a producer of the item, all FLMP for the run year:
    - `production_kg`;
    - `arable_m2_current` and `pasture_m2_current` (arable is feed land for animal products), and `land_use_current_m2`, their sum;
    - for animal products, `pasture_based_production_kg`, the scale of the pasture curve.
    - Downstream, the displaced share of production is `production_change_kg / production_kg`, and the relative land change is `land_use_m2 / land_use_current_m2`.
- `leakage_summary.csv`: one row per country × item × size.
  - For `land_use_m2`, `bd` and `ghg_coc_kgco2e`: the amount spared (`_spared`, minus the sum of C's displaced rows, which for animal products includes feed land abroad), leaked (`_leaked`, the sum of its replacement rows, C's own included), the net leakage (`_net`, leaked − spared) and their ratio (`_ratio`). Each has its percentiles and sd, from the same draws, so the betas shared between the two sides are accounted for. The ratio's spread is wide where the spared amount's spread is near zero; net leakage is better behaved.
  - FLMP errors spared and leaked, production GHG spared and leaked, `replaced_share` and `fallback_share`.
  - C's `displaced_kg`, `production_kg`, `land_use_current_m2` and `pasture_based_production_kg`.
- `{ISO}_leakage_per_kg.csv` (with `outputs.per_kg`): the same rows per kg displaced, at the central betas.
  - The linear impacts located there: arable and pasture land (m², always separate; pasture is 0 for crops, and arable for animal products is feed land), extinctions (`bd`) and carbon opportunity cost, with errors summed linearly, and production GHG at the location as a producer.
  - The extensified ones: `land_use_leakage_m2`, `bd_ext` and `ghg_coc_kgco2e_ext`.
- `{ISO}_leakage_per_m2.csv` (with `outputs.per_m2`): the per-kg rows per m² of land spared by C (the linear arable + pasture per kg of its displaced rows, wherever that land is).
- In the run folder, not `raw_outputs/`:
  - `config.json`: the run's record (see Recreating a run).
  - `inputs/impacts_matrices/`: the matrix script's outputs, when `paths.impacts_dir` is `null`.
