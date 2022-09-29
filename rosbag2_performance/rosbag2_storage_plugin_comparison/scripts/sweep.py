#!/usr/bin/env python3

from hmac import digest
from pathlib import Path
import subprocess
import shutil
from tempfile import mkdtemp
import csv
import sys
from io import StringIO

import yaml

CONFIG_DIMENSIONS = {
    "messages": {
        "large": { "topics": [{"name": "/large", "message_size": 1_000_000}]},
        "medium": { "topics": [{"name": "/medium", "message_size": 10_000}]},
        "small": { "topics": [{"name": "/small", "message_size": 100}]},
        "mixed": { "topics": [
            {"name": "/small", "message_size": 100, "write_proportion": 0.1},
            {"name": "/medium", "message_size": 10_000, "write_proportion": 0.2},
            {"name": "/large", "message_size": 1_000_000, "write_proportion": 0.7},
        ]},
    },
    "batch_size": {
        "small": { "min_batch_size_bytes": 1000 },
        "medium": { "min_batch_size_bytes": 1000000 },
        "large": { "min_batch_size_bytes": 100000000 },
    },
    "plugin_config": {
        "mcap_default": {"storage_id": "mcap"},
        "mcap_nocrc": {
            "storage_id": "mcap",
            "storage_options": {
                "noCRC": True,
            }
        },
        "mcap_nochunking": {
            "storage_id": "mcap",
            "storage_options": {
                "noCRC": True,
                "noChunking": True,
            }
        },
        "sqlite_default": {"storage_id": "sqlite3"},
        "sqlite_resilient": {
            "storage_id": "sqlite3",
            "storage_options": {
                "write": {"pragmas": [
                    "journal_mode=WAL",
                    "synchronous=NORMAL",
                ]},
            }
        },
    }
}


def executable_path():
    return Path(__file__).resolve().parent / "single_benchmark"

def build_configs():
    configs = [({}, {})]
    for dimension_name, dimension in CONFIG_DIMENSIONS.items():
        new_configs = []
        for (existing_name, existing_config) in configs:
            for variant_name, variant_fragment in dimension.items():
                label = f"{dimension_name}_{variant_name}"
                new_name = label if existing_name == "" else f"{existing_name}-{label}"
                new_name = dict(**existing_name)
                new_name[dimension_name] = variant_name
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
    return res.stdout.decode("utf-8")

def make_digest(name, csv_content):
    reader = csv.DictReader(StringIO(csv_content))
    write_times = []
    arena_sizes = []
    in_use_sizes = []
    byte_throughputs = []
    mmap_sizes = []
    close_time = None
    for row in reader:
        if row["close_ns"]:
            close_time = float(row["close_ns"]) / 1e9
        else:
            write_times.append(float(row["write_ns"]) / 1e9)
            arena_sizes.append(int(row["arena_bytes"]))
            byte_throughputs.append(float(row["num_bytes"]) / (float(row["write_ns"]) / 1e9))
            in_use_sizes.append(int(row["in_use_bytes"]))
            mmap_sizes.append(int(row["mmap_bytes"]))
    res = dict(**name)
    res.update({
        "name": "-".join([f"{k}={v}" for k, v in name.items()]),
        "avg_byte_throughput": sum(byte_throughputs) / len(byte_throughputs),
        "max_arena_size": max(arena_sizes),
        "max_in_use_size": max(in_use_sizes),
        "max_mmap_size": max(mmap_sizes),
        "close_time": close_time,
    })
    return res


def write_digest(outfile, digest_rows):
    writer = csv.DictWriter(
        outfile,
        list(digest_rows[0].keys()),
    )
    writer.writeheader()
    for row in digest_rows:
        writer.writerow(row)

if __name__ == "__main__":
    configs = build_configs()
    digest_rows = []
    for name, config in configs:
        print(f"Running benchmark: {name}...", file=sys.stderr)
        result = run_once(merged_config=config)
        digest_rows.append(make_digest(name, result))
    if len(sys.argv) > 1:
        with open(sys.argv[1], "w") as f:
            write_digest(f, digest_rows)
    else:
        s = StringIO()
        write_digest(s, digest_rows)
        print(s.getvalue())

    


    
    
