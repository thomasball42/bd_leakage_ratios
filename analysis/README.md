Analysis of this repository's runs. Each script takes the run's config, the repository's `config.json` by default, or another config or a run's record as its first argument, and reads the run's `raw_outputs/` and writes to its `processed/` (both in `{results_dir}/{run_name}/`).

## Flow matrices

`python build_flow_matrices.py [config.json] [--workers N]` writes one file per item to `processed/flow_matrices/`, `{item_code}_{item}.npz`, listed in `items.csv`. With 24 workers it takes a few minutes. It checks each item against `leakage_summary.csv`: the rows of its spared, leaked and ratio matrices must sum to the summary's spared and leaked amounts and leakage ratios.

`flow_matrices.py` loads them:

```python
import flow_matrices as fm
beef = fm.load(867)                                    # or by name
beef["sizes"]                                          # the run's displacement sizes
fm.matrix(beef, "bd")                                  # ratio matrix, at the smallest size
fm.matrix(beef, "land_use_m2", "net", size=0.01)       # at the run's size nearest 0.01
fm.matrix(beef, "bd", "per_kg", percentile=95)         # cells' percentiles over the draws
fm.leakage_ratio(beef, "bd")                           # the ratio rows' sums, with their percentiles
fm.totals(beef, "bd", "spared", percentile=5)          # what C spares (or leaks) in all: sizes x countries
fm.cube(beef, "bd", "share")                           # every size: sizes x countries x locations
```

Rows are the displacing countries C, columns the locations where the land is: a crop's where it's grown, an animal product's pasture where the animal is, and its feed's land where the feed was grown. For each of `land_use_m2`, `bd`, `ghg_coc_kgco2e` (extensified) and `ghg_prod_kgco2e` (linear, where the item is produced), the kinds are:

| Kind | Cell | Row sum |
|---|---|---|
| `per_kg` | impact leaked to the location, per kg displaced in C | leaked per kg |
| `spared` | impact spared at the location, per kg displaced in C | spared per kg |
| `net` | leaked − spared at the location, per kg | net leakage per kg |
| `ratio` | impact leaked to the location ÷ all C spares | C's leakage ratio, leaked ÷ spared |
| `share` | the location's share of all C leaks | 1 |

Things to know:
- **The C column** holds what changes in C: in `spared` C's own land, and in `per_kg` the land of the part of its displaced production C replaces itself (with `exclude_self: false`), which the run counts as leaked, and of any feed grown in C for the other replacing producers. `exclude_self=True` leaves the column out.
- **Spared isn't all in C.** For animal products, C's lost production frees its feed land wherever that was grown, so `spared` has cells abroad, and `net` shows where the spared and leaked feed land offset.
- **Ratios where little is spared.** Where C's beta is near 0 or negative (most pasture betas, and a few crops'), the spared amount is near 0 or negative, and the ratios are huge or change sign. `per_kg`, `net` and `share` don't have this problem.
- **Percentiles** are there for the `per_kg` and `spared` cells. The `ratio`, `share` and `net` cells have none, as they combine values whose draws aren't in the raw outputs; their rows' sums do (`leakage_ratio`). So do the `per_kg` and `spared` rows' sums, what C leaks and spares in all (`totals`), whose percentiles are their own, from the run's summary: summing the cells' percentiles wouldn't give them.
- **Size.** The smallest size is the marginal case. Larger ones change both the ratios and the shares, as each source's land responds to its own relative change.

Each file also has `replaced_share` (the share of C's production each location replaces as a producer, the same at every size), `displaced_kg`, each country's production and land, and each location's production of the item, central beta and fallback flags.

## Portfolio matrix

`python build_portfolio_matrices.py [config.json]`, once the flow matrices are built, writes `processed/flow_matrices/portfolio.npz`, in under a minute. It holds the same matrices, but per kg of each country's production portfolio displaced. Each row is C's row of every item it produces, weighted by C's FLMP production of the item:

P[size, C, location] = Σᵢ Qᵢ(C) · Mᵢ[size, C, location] / Σᵢ Qᵢ(C)

So a country that produces only wheat has the wheat matrix's row. Load it with `fm.load("portfolio")`. `fm.matrix`, `fm.cube`, `fm.leakage_ratio` and `figures/scatter_plot.py portfolio` work on it as on an item. It also has `items`, `weights` (countries × items, each item's share of C's production) and `covered_share` (sizes × countries, the share of C's production in its portfolio).

The script checks it against `leakage_summary.csv`: its rows must sum to C's leaked and spared amounts per kg displaced, weighted the same way.

Things to know:
- **Exact for share runs.** With `displacement.unit: "share"`, item i displaces size·Qᵢ(C) kg. So the portfolio is the sum of the items' scenarios when C displaces the same share of everything it produces, at every size. With `"kg"`, it weights each item's values at the same kg, which matches the portfolio only at marginal sizes. The items' scenarios are run separately, so land sources that two items share (a feed crop that C also displaces as a crop) aren't netted between them.
- **Gross production.** The weights are each item's full production. So feed grown in C for C's own animals counts both as the crop and in the animal products' feed, and its land is spared, and replaced, twice.
- **Weighted by kg.** Heavy, watery items such as sugar cane, milk and vegetables carry most of the weight where they're grown.
- **Items with no extensified results** are left out, and the remaining weights renormalised. These are items with no betas, and items at sizes where a land source would lose more than it produces. One set of items is used for every metric, so production GHG has the same portfolio. `covered_share` shows what's kept.
- **No percentiles.** A sum's percentiles can't be had from its terms', and the draws aren't saved. It does have central leakage ratios (`fm.leakage_ratio`).

## Scatter plots

In `figures/`, with what they share in `figures/_plot_funcs.py`. They write to `{figs_dir}/{run_name}/`, and each documents its options (`-h`).
- **`scatter_plot.py ITEM`:** one point per displacing country, at one size.
  - With the default `--axes flows`, x is what the displacement spares and y is what the replacement leaks, everywhere and C included. So y ÷ x is the leakage ratio.
  - `--axes domestic` plots the net leakage against the net change in C.
- **`scatter_plot_sizes.py ITEM`:** the same, over all the run's sizes, or those between `--min-size` and `--max-size`. Each country is a cross: the median over the sizes, with arms to their `--percentiles` (5 and 95 by default).
- **`--uncertainty`:** in both, adds ranges over the run's Monte Carlo draws, from `fm.totals`' percentiles. In `scatter_plot_sizes.py` they're combined with the spread over the sizes. It works with `--axes flows` and the extensified metrics only, and not for the portfolio.
