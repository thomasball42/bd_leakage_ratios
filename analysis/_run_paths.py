# -*- coding: utf-8 -*-
"""
Where a bd_leakage_ratios run's files are, from its config.json (or a run's record
of one, or its run folder): the run folder's raw_outputs, written by run_countries.py,
and processed, written by the analysis scripts here, and the figures folder. The
config's relative paths are from the repository folder, as they are there.
"""

import copy
import functools
import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_DIR))  # for _run_config, and _leakage_funcs where needed

import _run_config  # noqa: E402

DEFAULT_CONFIG = REPO_DIR / "config.json"
PROCESSED = "processed"  # the run folder's subfolder for the analysis outputs
# the scripts' help for their config argument
CONFIG_HELP = ("the run: its folder ({results_dir}/{run_name}), its record in there (config.json), or a config "
               "(default: the repository's config.json)")


def run_paths(config_path: Path = DEFAULT_CONFIG) -> tuple[dict, Path, Path, Path]:
    """The settings in `config_path`, and its run's raw_outputs, processed and data
    folders."""
    config, run_dir = _load(Path(config_path).resolve())
    return (copy.deepcopy(config), run_dir / _run_config.RAW_OUTPUTS, run_dir / PROCESSED,
            _run_config.resolve(config["paths"]["data_dir"]))


def figs_dir(config_path: Path = DEFAULT_CONFIG) -> Path:
    """The run's figures folder, {figs_dir}/{run folder's name} from the config at
    `config_path`, with figs_dir {results_dir}/figures where it is null."""
    config, run_dir = _load(Path(config_path).resolve())
    paths = config["paths"]
    return _run_config.resolve(paths["figs_dir"] or str(Path(paths["results_dir"]) / "figures")) / run_dir.name


@functools.cache
def _load(path: Path) -> tuple[dict, Path]:
    """The settings at `path` and the run folder. A run folder's settings are its
    record's, and it's the run folder wherever the record says; a config's is
    {results_dir}/{run_name}. Loaded once, so a record's warnings show once."""
    if path.is_dir():
        if not (path / "config.json").is_file():
            raise FileNotFoundError(f"{path} has no config.json, so isn't a run folder")
        return _run_config.load(path / "config.json"), path
    config = _run_config.load(path)
    return config, _run_config.resolve(config["paths"]["results_dir"]) / config["run"]["run_name"]
