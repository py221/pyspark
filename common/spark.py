"""Shared helpers: a local SparkSession builder and repo paths.

Every example script calls ``get_spark()`` so the configuration lives in one
place. The settings are tuned for a laptop: few shuffle partitions, no web UI,
and all generated files under ``output/`` (which is git-ignored).
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
OUTPUT = ROOT / "output"


def data_path(name: str) -> str:
    """Absolute path (as a string) of a file under data/."""
    return str(DATA / name)


def output_path(*parts: str) -> str:
    """Absolute path (as a string) under output/. Parent dirs are created."""
    p = OUTPUT.joinpath(*parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


def fresh_dir(*parts: str) -> str:
    """Like output_path, but deletes whatever was there first.

    Handy for streaming checkpoints, which must start clean to re-run a script.
    """
    p = OUTPUT.joinpath(*parts)
    if p.exists():
        shutil.rmtree(p)
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


def get_spark(app_name: str = "learning-path", shuffle_partitions: int = 4,
              cores: int = 2) -> SparkSession:
    """Build (or reuse) a local SparkSession.

    * ``local[N]``       - driver and executors share one JVM
    * shuffle partitions - default 200 is silly for tiny data
    * UTC session zone   - timestamps print the same on every machine
    * warehouse dir      - managed tables land in output/warehouse
    """
    OUTPUT.mkdir(exist_ok=True)
    # make Python workers use the same interpreter as the driver (venv-safe)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(cores))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(OUTPUT / "warehouse"))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
