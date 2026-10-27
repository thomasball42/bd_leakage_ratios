# -*- coding: utf-8 -*-
"""
Where a bd_leakage_ratios run's files are, from its config.json (or a run's record
of one): the run folder's raw_outputs, written by run_countries.py, and processed,
written by the analysis scripts here, and the figures folder. The config's relative paths are from the
repository folder, as they are there.
"""

import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_DIR))  # for _run_config, and _leakage_funcs where needed

import _run_config  # noqa: E402

DEFAULT_CONFIG = REPO_DIR / "config.json"
PROCESSED = "processed"  # the run folder's subfolder for the analysis outputs


def run_paths(config_path: Path = DEFAULT_CONFIG) -> tuple[dict, Path, Path, Path]:
    """The settings in `config_path`, and its run's raw_outputs, processed and data
    folders."""
    config = _run_config.load(config_path)
    run_dir = _run_config.resolve(config["paths"]["results_dir"]) / config["run"]["run_name"]
    return (config, run_dir / _run_config.RAW_OUTPUTS, run_dir / PROCESSED,
            _run_config.resolve(config["paths"]["data_dir"]))


def figs_dir(config_path: Path = DEFAULT_CONFIG) -> Path:
    """The run's figures folder, {figs_dir}/{run_name} from the config at
    `config_path`, with figs_dir {results_dir}/figures where it is null."""
    config = _run_config.load(config_path)
    paths = config["paths"]
    return (_run_config.resolve(paths["figs_dir"] or str(Path(paths["results_dir"]) / "figures"))
            / config["run"]["run_name"])
