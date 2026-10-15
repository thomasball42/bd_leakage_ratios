"""
Downloads the extensification factors ('beta', the elasticity between a country's
farmed area and its production of each item, from FAOSTAT via a Kalman filter)
from Zenodo into data/: beta_crops.csv and beta_animals.csv.

The file list and md5 checksums come from the Zenodo API, so files already in
data/ with the right checksum are skipped. Any archives (.zip, .tar.*, .gz) are
extracted into data/ after downloading.

https://zenodo.org/records/23190337
"""

import argparse
import gzip
import hashlib
import shutil
import tarfile
import zipfile
from pathlib import Path

import requests
from tqdm import tqdm

RECORD_ID = "23190337"
DATA_DIR = Path("data")


def md5_of(path: Path) -> str:
    md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            md5.update(chunk)
    return md5.hexdigest()


def download(url: str, out_path: Path, size: int) -> None:
    """Stream url to out_path, via a .part file so an interrupted download isn't
    mistaken for a finished one."""
    part_path = out_path.with_name(out_path.name + ".part")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(part_path, "wb") as f, tqdm(total=size, unit="B", unit_scale=True, desc=out_path.name) as bar:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                bar.update(len(chunk))
    part_path.replace(out_path)


def extract(path: Path, data_dir: Path) -> None:
    """Extract path into data_dir if it's an archive, otherwise do nothing."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as z:
            z.extractall(data_dir)
    elif tarfile.is_tarfile(path):
        with tarfile.open(path) as t:
            t.extractall(data_dir, filter="data")
    elif path.suffix == ".gz":
        with gzip.open(path, "rb") as f_in, open(data_dir / path.stem, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    else:
        return
    print(f"Extracted {path.name} into {data_dir}")


def main(record_id: str = RECORD_ID, data_dir: Path = DATA_DIR) -> list[Path]:
    """Download every file in the Zenodo record into data_dir, extract any
    archives, and return the downloaded paths."""

    data_dir = Path(data_dir)
    data_dir.mkdir(exist_ok=True, parents=True)

    r = requests.get(f"https://zenodo.org/api/records/{record_id}", timeout=60)
    r.raise_for_status()
    files = r.json()["files"]

    paths = []
    for file in files:
        out_path = data_dir / file["key"]
        algorithm, checksum = file["checksum"].split(":")
        assert algorithm == "md5", f"Unexpected checksum algorithm for {file['key']}: {algorithm}"

        if out_path.exists() and md5_of(out_path) == checksum:
            print(f"{out_path} already downloaded")
        else:
            download(file["links"]["self"], out_path, file["size"])
            if md5_of(out_path) != checksum:
                raise ValueError(f"Checksum mismatch for {out_path}, delete it and rerun")
            extract(out_path, data_dir)

        paths.append(out_path)

    return paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--record-id", default=RECORD_ID)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args()

    main(args.record_id, args.data_dir)
