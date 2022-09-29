#!/usr/bin/env python3

from pathlib import Path
import subprocess
import shutil
from tempfile import mkdtemp

import yaml

CONFIG_DIMENSIONS = {
    "messages": {
        "all_large": { "topics": [{"name": "/large", "message_size": 1_000_000}]},
        "all_medium": { "topics": [{"name": "/medium", "message_size": 10_000}]},
        "all_small": { "topics": [{"name": "/small", "message_size": 10}]},
        "mixed": { "topics": [
            {"name": "/small", "message_size": 10},
            {"name": "/medium", "message_size": 10_000},
            {"name": "/large", "message_size": 1_000_000},
        ]},
    },
    "plugin_config": {
        "mcap_default": {"storage_id": "mcap"},
        "mcap_nocrc": {
            "storage_id": "mcap",
            "storage_options": {
                "noCRC": True,
            }
        },
        "sqlite_default": {"storage_id": "sqlite3"},
        "sqlite_resilient": {
            "storage_id": "sqlite3",
            "storage_options": {
                "journal_mode": "PRAGMA journal_mode=WAL;",
                "synchronous": "PRAGMA synchronous=NORMAL;",
            }
        },
    }
}


def executable_path():
    return Path(__file__).resolve().parent / "single_benchmark"

def build_configs():
    configs = [("", {})]
    for dimension in CONFIG_DIMENSIONS.values():
        new_configs = []
        for (existing_name, existing_config) in configs:
            for variant_name, variant_fragment in dimension.items():
                new_name = variant_name if existing_name == "" else f"{existing_name}-{variant_name}"
                new_config = dict(**existing_config)
                new_config.update(variant_fragment)
                new_configs.append((new_name, new_config))
        configs = new_configs
    return configs

def run_once(merged_config):
    outdir = Path(mkdtemp())
    try:
        res = subprocess.run(
            [executable_path(), yaml.dump(merged_config), outdir],
            check=True,
            stdout=subprocess.PIPE
        )
    finally:
        shutil.rmtree(outdir)
    return res.stdout

if __name__ == "__main__":
    configs = build_configs()
    for name, config in configs:
        result = run_once(merged_config=config)
        with open(f"{name}.csv", "wb") as f:
            f.write(result)
