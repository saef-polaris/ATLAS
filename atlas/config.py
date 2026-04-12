from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = ROOT.parent.parent / "ATLAS-data"
DEFAULT_DATA_REPO = "https://github.com/saef-polaris/ATLAS-data.git"


def get_data_dir() -> Path:
    return Path(os.environ.get("ATLAS_DATA_DIR", str(DEFAULT_DATA_DIR))).expanduser().resolve()


def get_marker_dir() -> Path:
    return get_data_dir() / "marker_runs"


def data_dir_is_ready(data_dir: Path | None = None) -> bool:
    target = (data_dir or get_data_dir()).expanduser().resolve()
    if target.name == "marker_runs":
        return target.exists()
    return (target / "marker_runs").exists()


def ensure_data_dir(
    data_dir: Path | None = None,
    repo_url: str = DEFAULT_DATA_REPO,
    clone_if_missing: bool = False,
) -> Path:
    target = (data_dir or get_data_dir()).expanduser().resolve()
    if data_dir_is_ready(target):
        return target
    if clone_if_missing:
        if target.exists() and any(target.iterdir()):
            raise FileExistsError(
                f"Data directory exists but is not usable and is not empty: {target}"
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "clone", repo_url, str(target)], check=True)
        if data_dir_is_ready(target):
            return target
        raise FileNotFoundError(f"Cloned data repo but marker_runs is still missing: {target}")
    expected = target if target.name == "marker_runs" else target / "marker_runs"
    raise FileNotFoundError(
        "ATLAS data directory not found. Set ATLAS_DATA_DIR or run `atlas init-data`. "
        f"Expected marker_runs under: {expected}"
    )
