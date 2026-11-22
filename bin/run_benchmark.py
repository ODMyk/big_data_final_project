#!/usr/bin/env python3
import json
import os
import re
import subprocess
import time
import urllib.request
import statistics

WARMUP_RUNS = int(os.environ.get("NUM_WARMUP", 1))
BENCHMARK_RUNS = int(os.environ.get("NUM_RUNS", 3))
OUTPUT_JSON = os.environ.get("OUTPUT_JSON", "benchmark_results.json")
INPUT_CSV = os.environ.get("INPUT_CSV", "/opt/spark/work/data/yellow_tripdata_12m.csv")
HISTORY_API = "http://localhost:18080/api/v1/applications"

CONFIGURATIONS = [
    {
        "name": "Baseline",
        "script": "/opt/spark/work/pipeline.py",
        "output_dir": "/artifacts/benchmark_baseline",
    },
    {
        "name": "Cached",
        "script": "/opt/spark/work/pipeline_cached.py",
        "output_dir": "/artifacts/benchmark_cached",
    },
]


def get_latest_app_id(after_epoch_ms=0):
    """Wait for a newly completed application in the History Server."""
    for _ in range(60):
        try:
            req = urllib.request.urlopen(HISTORY_API, timeout=5)
            apps = json.loads(req.read().decode())
            for app in apps:
                attempts = app.get("attempts", [])
                if attempts and attempts[0].get("startTimeEpoch", 0) >= after_epoch_ms:
                    if attempts[0].get("completed", False):
                        return app["id"]
        except Exception:
            pass
        time.sleep(2)
    return None


def fetch_app_metrics(app_id):
    """Fetch input, shuffle, and spill metrics for app_id from History Server."""
    base = f"{HISTORY_API}/{app_id}"
    app_info = json.loads(urllib.request.urlopen(f"{base}").read().decode())
    duration_ms = app_info["attempts"][0]["duration"]

    stages = json.loads(urllib.request.urlopen(f"{base}/stages").read().decode())
    total_input = sum(s.get("inputBytes", 0) for s in stages if s.get("status") == "COMPLETE")
    total_shuf_read = sum(s.get("shuffleReadBytes", 0) for s in stages if s.get("status") == "COMPLETE")
    total_shuf_write = sum(s.get("shuffleWriteBytes", 0) for s in stages if s.get("status") == "COMPLETE")
    total_mem_spill = sum(s.get("memoryBytesSpilled", 0) for s in stages if s.get("status") == "COMPLETE")
    total_disk_spill = sum(s.get("diskBytesSpilled", 0) for s in stages if s.get("status") == "COMPLETE")

    return {
        "app_id": app_id,
        "duration_s": duration_ms / 1000.0,
        "input_bytes": total_input,
        "input_gib": total_input / (1024 ** 3),
        "shuffle_read_bytes": total_shuf_read,
        "shuffle_read_gib": total_shuf_read / (1024 ** 3),
        "shuffle_write_bytes": total_shuf_write,
        "shuffle_write_gib": total_shuf_write / (1024 ** 3),
        "mem_spill_bytes": total_mem_spill,
        "mem_spill_gib": total_mem_spill / (1024 ** 3),
        "disk_spill_bytes": total_disk_spill,
        "disk_spill_gib": total_disk_spill / (1024 ** 3),
    }


def parse_spills_from_output(output_text):
    """Count UnsafeExternalSorter spills and calculate total spilled GiB from log."""
    matches = re.findall(r"spilling sort data of ([\d\.]+) MiB to disk", output_text)
    count = len(matches)
    total_mib = sum(float(m) for m in matches)
    return count, total_mib / 1024.0


def run_single(config, is_warmup=False, run_idx=0):
    desc = "Warmup" if is_warmup else f"Run {run_idx}"
    print(f"  [{config['name']}] Starting {desc}...")

    # Clean destination directory
    subprocess.run(
        ["docker", "compose", "exec", "-u", "0", "spark", "rm", "-rf", config["output_dir"]],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    cmd = [
        "docker", "compose", "exec", "-T",
        "-e", f"INPUT_CSV={INPUT_CSV}",
        "-e", f"OUTPUT_DIR={config['output_dir']}",
        "spark", "/opt/spark/bin/spark-submit", config["script"],
    ]

    start_epoch_ms = int(time.time() * 1000)
    wall_start = time.time()
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    wall_duration = time.time() - wall_start

    if res.returncode != 0:
        print(f"  [{config['name']}] FAILED with return code {res.returncode}")
        print(res.stdout[-1000:])
        raise RuntimeError(f"Run failed for {config['name']}")

    # Extract TIME: prefix if present
    time_match = re.search(r"TIME:\s*([\d\.]+)", res.stdout)
    reported_time = float(time_match.group(1)) if time_match else wall_duration

    # Count spills
    spill_count, spill_gib = parse_spills_from_output(res.stdout)

    # Fetch from history server
    app_id = get_latest_app_id(start_epoch_ms - 2000)
    api_metrics = {}
    if app_id:
        try:
            api_metrics = fetch_app_metrics(app_id)
        except Exception as e:
            print(f"    Warning: failed to fetch API metrics: {e}")

    duration_final = api_metrics.get("duration_s", reported_time)
    input_gib = api_metrics.get("input_gib", 0.0)
    shuffle_gib = api_metrics.get("shuffle_read_gib", 0.0)

    print(
        f"  [{config['name']}] {desc} finished in {duration_final:.2f}s "
        f"(Read: {input_gib:.2f} GiB, Shuffle: {shuffle_gib:.2f} GiB, "
        f"Spills: {spill_gib:.2f} GiB ({spill_count}))"
    )

    return {
        "config": config["name"],
        "is_warmup": is_warmup,
        "run_idx": run_idx,
        "duration_s": duration_final,
        "wall_duration_s": wall_duration,
        "reported_time_s": reported_time,
        "input_gib": input_gib,
        "shuffle_read_gib": shuffle_gib,
        "spill_gib": spill_gib,
        "spill_count": spill_count,
        "api_metrics": api_metrics,
    }


def main():
    print(f"Starting benchmark: WARMUP={WARMUP_RUNS}, RUNS={BENCHMARK_RUNS}")
    all_results = []

    for config in CONFIGURATIONS:
        print(f"\n==========================================")
        print(f"Benchmarking: {config['name']}")
        print(f"==========================================")

        for w in range(WARMUP_RUNS):
            run_single(config, is_warmup=True, run_idx=w + 1)

        config_runs = []
        for r in range(BENCHMARK_RUNS):
            res = run_single(config, is_warmup=False, run_idx=r + 1)
            config_runs.append(res)
            all_results.append(res)

        durations = [r["duration_s"] for r in config_runs]
        reads = [r["input_gib"] for r in config_runs]
        shuffles = [r["shuffle_read_gib"] for r in config_runs]
        spills = [r["spill_gib"] for r in config_runs]
        spill_cnts = [r["spill_count"] for r in config_runs]

        print(f"\n--- {config['name']} Summary ---")
        print(f"Times: {[round(d, 2) for d in durations]} | Median: {statistics.median(durations):.2f}s")
        print(f"Read: {[round(x, 2) for x in reads]} GiB | Median: {statistics.median(reads):.2f} GiB")
        print(f"Shuffle: {[round(x, 2) for x in shuffles]} GiB | Median: {statistics.median(shuffles):.2f} GiB")
        print(f"Spill: {[round(x, 2) for x in spills]} GiB | Median: {statistics.median(spills):.2f} GiB")

        # Save checkpoint
        with open(OUTPUT_JSON, "w") as f:
            json.dump(all_results, f, indent=2)

    print(f"\nBenchmark completed successfully! Results saved to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()

