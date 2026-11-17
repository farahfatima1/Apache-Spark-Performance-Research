
import csv
import json
import statistics
import subprocess
import sys
from pathlib import Path

BASE = Path(r"C:\SparkResearch")
CHILD = BASE / "rq_remeasure_child.py"
RESULTS = BASE / "results"
OUT = RESULTS / "rq1_rq4_remeasured.csv"
SUMMARY = RESULTS / "rq1_rq4_summary.csv"

SIZES = [100000, 500000, 1000000]
CPUS = ["local[1]", "local[2]", "local[4]", "local[*]"]
FORMATS = ["CSV", "PARQUET"]
TRIALS = [1, 2, 3]

EXPECTED = {
    100000: 6751,
    500000: 33106,
    1000000: 66981,
}

RAW_FIELDS = [
    "format", "rows", "cpu", "trial",
    "seconds", "peak_rss_mb", "matched"
]

SUMMARY_FIELDS = [
    "format", "rows", "cpu", "n",
    "seconds_mean", "seconds_sd",
    "peak_rss_mb_mean", "peak_rss_mb_sd"
]

RESULTS.mkdir(parents=True, exist_ok=True)
results = []


def save_raw():
    with OUT.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_FIELDS)
        writer.writeheader()
        writer.writerows(results)


try:
    if not CHILD.is_file():
        raise FileNotFoundError(f"Missing child script: {CHILD}")

    # Check all input files before starting any Spark processes.
    for size in SIZES:
        for fmt in FORMATS:
            if fmt == "CSV":
                path = BASE / "data" / f"events_{size}.csv"
            else:
                path = (
                    BASE / "data" / "parquet" /
                    f"events_{size}.parquet"
                )
            if not path.is_file():
                raise FileNotFoundError(f"Missing dataset: {path}")

    for size in SIZES:
        for cpu in CPUS:
            for trial in TRIALS:
                # Trials 1 and 3: CSV first.
                # Trial 2: Parquet first.
                order = (
                    FORMATS if trial % 2 == 1
                    else list(reversed(FORMATS))
                )

                for fmt in order:
                    print(
                        f"\nDataset={size}, CPU={cpu}, "
                        f"Format={fmt}, Trial={trial}",
                        flush=True
                    )

                    proc = subprocess.run(
                        [
                            sys.executable, str(CHILD),
                            str(size), fmt, cpu, str(trial)
                        ],
                        cwd=str(BASE),
                        capture_output=True,
                        text=True
                    )

                    if proc.returncode != 0:
                        print("CHILD PROCESS FAILED")
                        print(proc.stdout)
                        print(proc.stderr)
                        raise RuntimeError(
                            f"Trial failed: {size}, {cpu}, "
                            f"{fmt}, trial {trial}"
                        )

                    json_lines = [
                        line for line in proc.stdout.splitlines()
                        if line.strip().startswith("{")
                    ]
                    if not json_lines:
                        print(proc.stdout)
                        print(proc.stderr)
                        raise RuntimeError("No JSON result returned")

                    record = json.loads(json_lines[-1])

                    if record["matched"] != EXPECTED[size]:
                        raise RuntimeError(
                            f"Unexpected count: {record}"
                        )

                    results.append(record)
                    save_raw()
                    print(record, flush=True)

    if len(results) != 72:
        raise RuntimeError(
            f"Expected 72 trials, received {len(results)}"
        )

    # Build summaries for each format, size and CPU configuration.
    summary = []
    outliers = []

    for size in SIZES:
        for cpu in CPUS:
            for fmt in FORMATS:
                group = [
                    r for r in results
                    if r["rows"] == size
                    and r["cpu"] == cpu
                    and r["format"] == fmt
                ]

                if len(group) != 3:
                    raise RuntimeError(
                        f"Expected 3 trials in group: "
                        f"{size}, {cpu}, {fmt}"
                    )

                times = [r["seconds"] for r in group]
                rss = [r["peak_rss_mb"] for r in group]
                median_time = statistics.median(times)

                for r in group:
                    if median_time > 0 and r["seconds"] > 3 * median_time:
                        outliers.append(r)

                summary.append({
                    "format": fmt,
                    "rows": size,
                    "cpu": cpu,
                    "n": len(group),
                    "seconds_mean": round(statistics.mean(times), 6),
                    "seconds_sd": round(statistics.stdev(times), 6),
                    "peak_rss_mb_mean": round(statistics.mean(rss), 2),
                    "peak_rss_mb_sd": round(statistics.stdev(rss), 2)
                })

    with SUMMARY.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows(summary)

    print("\n=== VALIDATION ===")
    print(f"Raw trials: {len(results)}")
    print("All match counts are correct.")
    print("All groups contain three trials.")
    print(f"Raw results: {OUT}")
    print(f"Summary: {SUMMARY}")

    print("\n=== TRIALS OVER 3x THEIR GROUP MEDIAN TIME ===")
    if outliers:
        for r in outliers:
            print(r)
    else:
        print("None")

except Exception as exc:
    # Preserve completed measurements if the run fails.
    save_raw()
    print(f"\nSTOPPED: {exc}")
    print(f"Completed raw trials preserved: {len(results)}")
    print(f"Partial raw results: {OUT}")
    sys.exit(1)
