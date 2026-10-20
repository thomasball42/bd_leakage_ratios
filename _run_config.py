"""
Reads a run's config.json, and writes the run's record: a copy of that config with
a provenance section (the repository version, any uncommitted changes, the Python
environment and the checksums of the input files), into the run's results folder.
The run can then be recreated from the record alone, by checking out the recorded
commit and running `python run_countries.py <results_dir>/<run_name>/config.json`
(with a new run_name or results_dir, so as not to overwrite it).
"""

import hashlib
import json
import platform
import re
import subprocess
import sys
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent
RECORD_KEY = "provenance"  # the section a run's record adds to its config
# every setting, by group (see the README); a config must have exactly these
SETTINGS = {
    "run": ["run_name", "year", "countries"],
    "paths": ["flmp_results_dir", "results_dir", "impacts_dir", "data_dir"],
    "displacement": ["unit", "sizes", "sizes_dist", "sizes_n", "sizes_range"],
    "replacement": ["comprehensive", "exclude_self"],
    "extensification": ["use_beta", "value", "std", "clipping"],
    "monte_carlo": ["draws", "seed", "percentiles"],
    "outputs": ["per_kg", "per_m2"],
}
PACKAGES = ["numpy", "pandas", "tqdm"]
MAX_UNTRACKED_BYTES = 1 << 20  # untracked files up to this size go in the record's patch, bigger ones are only listed


def load(path: Path) -> dict:
    """The settings in the config at `path`. A run's record can be loaded too: its
    provenance is checked against the current repository and otherwise ignored."""
    config = json.loads(Path(path).read_text())
    record = config.pop(RECORD_KEY, None)

    problems = [f"{group!r} missing" for group in SETTINGS if group not in config]
    problems += [f"{group!r} isn't a setting group" for group in config if group not in SETTINGS]
    for group, keys in SETTINGS.items():
        given = config.get(group, {})
        problems += [f"{group}.{key} missing" for key in keys if key not in given]
        problems += [f"{group}.{key} isn't a setting" for key in given if key not in keys]
    if problems:
        raise ValueError(f"{path}: " + "; ".join(problems))
    run_name = config["run"]["run_name"]
    if not isinstance(run_name, str) or not run_name or "/" in run_name:
        raise ValueError(f"{path}: run.run_name must be a folder name, not {run_name!r}")
    def number(x):
        return isinstance(x, (int, float)) and not isinstance(x, bool)

    unit, sizes = config["displacement"]["unit"], config["displacement"]["sizes"]
    if unit not in ("kg", "share"):
        problems.append(f"displacement.unit must be 'kg' or 'share', not {unit!r}")
    if not isinstance(sizes, list) or not all(number(s) and s > 0 for s in sizes):
        problems.append(f"displacement.sizes must be a list of numbers above 0, or empty, not {sizes!r}")
    use_beta, value, std = (config["extensification"][key] for key in ["use_beta", "value", "std"])
    if not isinstance(use_beta, bool):
        problems.append(f"extensification.use_beta must be true or false, not {use_beta!r}")
    elif not use_beta:
        if not number(value):
            problems.append(f"extensification.value must be a number when use_beta is false, not {value!r}")
        if not number(std) or std < 0:
            problems.append(f"extensification.std must be a number from 0 when use_beta is false, not {std!r}")
    draws, seed, percentiles = (config["monte_carlo"][key] for key in ["draws", "seed", "percentiles"])
    if not isinstance(draws, int) or isinstance(draws, bool) or draws < 0 or draws == 1:
        problems.append(f"monte_carlo.draws must be 0 (central values only) or a whole number from 2, not {draws!r}")
    if not isinstance(seed, int) or isinstance(seed, bool):
        problems.append(f"monte_carlo.seed must be a whole number, not {seed!r}")
    if not isinstance(percentiles, list) or not all(number(q) and 0 <= q <= 100 for q in percentiles):
        problems.append(f"monte_carlo.percentiles must be a list of numbers from 0 to 100, not {percentiles!r}")
    for key in SETTINGS["outputs"]:
        if not isinstance(config["outputs"][key], bool):
            problems.append(f"outputs.{key} must be true or false, not {config['outputs'][key]!r}")
    if problems:
        raise ValueError(f"{path}: " + "; ".join(problems))

    if record is not None:
        recorded, current = record["repository"], repository()
        if recorded["commit"] != current["commit"]:
            print(f"Warning: {path} was recorded at commit {recorded['commit']}, but the repository is at "
                  f"{current['commit']}; check that commit out to recreate the run")
        if recorded["uncommitted_changes"]:
            print(f"Warning: the run recorded in {path} had uncommitted changes, in "
                  f"{RECORD_KEY}.repository.uncommitted_changes (apply them with git apply)")
    return config


def resolve(path: str | None) -> Path | None:
    """A config path, relative ones taken from the repository folder."""
    return None if path is None else REPO_DIR / path


def check_run_dir(config: dict, run_dir: Path) -> None:
    """Refuse to reuse a run folder that holds a run with different settings, so a
    record always describes the results next to it."""
    record_path = run_dir / "config.json"
    if not record_path.exists():
        return
    previous = json.loads(record_path.read_text())
    previous.pop(RECORD_KEY, None)
    if previous != config:
        raise ValueError(f"{run_dir} already holds a run with different settings (see {record_path}); "
                         f"use another run_name, or delete that folder")


def _git(*args: str, strip: bool = True, ok_codes: tuple[int, ...] = (0,)) -> str | None:
    try:
        result = subprocess.run(["git", "-C", str(REPO_DIR), *args], capture_output=True, text=True, errors="replace")
    except OSError:
        return None
    if result.returncode not in ok_codes:
        return None
    return result.stdout.strip() if strip else result.stdout


def repository() -> dict:
    """The repository's remote (without any credentials), branch and commit, whether
    that commit is on the remote, and any uncommitted changes as one patch on that
    commit: tracked files' changes, plus untracked files up to MAX_UNTRACKED_BYTES
    as new files. All untracked files are listed too."""
    remote = _git("remote", "get-url", "origin")
    status = _git("status", "--porcelain", "--untracked-files=all") or ""
    untracked = [line[3:] for line in status.splitlines() if line.startswith("??")]
    patch = _git("diff", "--binary", "HEAD", strip=False) or ""
    for path in untracked:
        if (REPO_DIR / path).stat().st_size <= MAX_UNTRACKED_BYTES:
            # exit code 1 means the file differs from /dev/null, i.e. the patch that creates it
            patch += _git("diff", "--no-index", "--binary", "--", "/dev/null", path, strip=False, ok_codes=(1,)) or ""
    return {
        "remote": re.sub(r"//[^@/]+@", "//", remote) if remote else None,
        "branch": _git("branch", "--show-current"),
        "commit": _git("rev-parse", "HEAD"),
        "commit_on_remote": bool(_git("branch", "--remotes", "--contains", "HEAD")),
        "uncommitted_changes": patch or None,
        "untracked_files": untracked,
    }


def md5_of(path: Path) -> str:
    md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            md5.update(chunk)
    return md5.hexdigest()


def write_record(config: dict, config_path: Path, run_dir: Path, inputs: list[Path]) -> Path:
    """Write the run's record, `config` plus its provenance, to run_dir/config.json
    and return its path. `inputs` are the files the run read, checksummed."""
    repo = repository()
    if repo["uncommitted_changes"]:
        print("Warning: the repository has uncommitted changes; they're saved in the record, "
              "but committing first makes the run easier to recreate")
    elif not repo["commit_on_remote"]:
        print(f"Warning: commit {repo['commit']} isn't on the remote yet; push it so the run can be recreated elsewhere")

    record = {**config, RECORD_KEY: {
        "completed": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "config_file": str(Path(config_path).resolve()),
        "command": " ".join([Path(sys.executable).name] + sys.argv),
        "repository": repo,
        "environment": {
            "python": platform.python_version(),
            "packages": {package: metadata.version(package) for package in PACKAGES},
        },
        "inputs_md5": {str(path): md5_of(path) for path in inputs},
    }}
    record_path = run_dir / "config.json"
    record_path.write_text(json.dumps(record, indent=2) + "\n")
    return record_path
