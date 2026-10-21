Analysis of this repository's runs. Each script takes the run's config, the repository's `config.json` by default, or another config or a run's record as its first argument, and reads the run's `raw_outputs/` and writes to its `processed/` (both in `{results_dir}/{run_name}/`).

## Flow matrices

`python build_flow_matrices.py [config.json] [--workers N]` writes one file per item to `processed/flow_matrices/`, `{item_code}_{item}.npz`, listed in `items.csv`. With 24 workers it takes a few minutes. It checks each item against `leakage_summary.csv`: the rows of its spared and ratio matrices must sum to the summary's spared amounts and leakage ratios.

`flow_matrices.py` loads them:

```python
import flow_matrices as fm
beef = fm.load(867)                                    # or by name
beef["sizes"]                                          # the run's displacement sizes
fm.matrix(beef, "bd")                                  # ratio matrix, at the smallest size
fm.matrix(beef, "land_use_m2", "net", size=0.01)       # at the run's size nearest 0.01
fm.matrix(beef, "bd", "per_kg", percentile=95)         # cells' percentiles over the draws
fm.leakage_ratio(beef, "bd")                           # the ratio rows' sums, with their percentiles
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
- **Percentiles** are there for the `per_kg` and `spared` cells. The `ratio`, `share` and `net` cells have none, as they combine values whose draws aren't in the raw outputs; their rows' sums do (`leakage_ratio`).
- **Size.** The smallest size is the marginal case. Larger ones change both the ratios and the shares, as each source's land responds to its own relative change.

Each file also has `replaced_share` (the share of C's production each location replaces as a producer, the same at every size), `displaced_kg`, each country's production and land, and each location's production of the item, central beta and fallback flags.
