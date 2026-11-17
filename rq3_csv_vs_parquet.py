
import csv
import time
import threading
from pathlib import Path

import pandas as pd
import psutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BASE = Path(r"C:\SparkResearch")
DATA = BASE / "data"
RESULTS = BASE / "results"
PARQUET = DATA / "parquet"

RESULTS.mkdir(parents=True, exist_ok=True)
PARQUET.mkdir(parents=True, exist_ok=True)

datasets = {
    100_000: DATA / "events_100000.csv",
    500_000: DATA / "events_500000.csv",
    1_000_000: DATA / "events_1000000.csv",
}

spark = (
    SparkSession.builder
    .appName("RQ3_CSV_vs_Parquet")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

process = psutil.Process()
stop_monitor = threading.Event()
peak_mb = [0.0]


def monitor_memory():
    while not stop_monitor.is_set():
        try:
            processes = [process] + process.children(recursive=True)
            total = sum(
                p.memory_info().rss
                for p in processes
                if p.is_running()
            )
            peak_mb[0] = max(peak_mb[0], total / (1024 * 1024))
        except (psutil.Error, OSError):
            pass
        stop_monitor.wait(0.05)


def folder_size(path):
    if path.is_file():
        return path.stat().st_size
    return sum(
        p.stat().st_size
        for p in path.rglob("*")
        if p.is_file()
    )


def read_data(path, fmt):
    if fmt == "CSV":
        return (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(path))
        )
    return spark.read.parquet(str(path))


def run_query(path, fmt):
    peak_mb[0] = 0.0
    stop_monitor.clear()

    monitor = threading.Thread(
        target=monitor_memory,
        daemon=True
    )
    monitor.start()

    try:
        start = time.perf_counter()

        df = read_data(path, fmt)

        matched = df.filter(
            (col("event_type") == "alert") &
            (col("status") == "failure")
        ).count()

        elapsed = time.perf_counter() - start

    finally:
        stop_monitor.set()
        monitor.join()

    del df

    return elapsed, matched, peak_mb[0]


rows = []

try:
    for size, csv_path in datasets.items():
        if not csv_path.exists():
            raise FileNotFoundError(
                f"Dataset not found: {csv_path}"
            )

        parquet_path = PARQUET / f"events_{size}.parquet"

        print(f"\n{'=' * 55}")
        print(f"Dataset: {size:,} rows")
        print(f"{'=' * 55}")

        # Create Parquet with PyArrow to avoid Spark's
        # Windows Hadoop filesystem write-permission issue.
        start = time.perf_counter()

        pd.read_csv(csv_path).to_parquet(
            parquet_path,
            engine="pyarrow",
            compression="snappy",
            index=False
        )

        conversion_seconds = time.perf_counter() - start

        csv_bytes = csv_path.stat().st_size
        parquet_bytes = folder_size(parquet_path)

        print(f"Parquet conversion: {conversion_seconds:.3f} s")
        print(f"CSV size: {csv_bytes / 1e6:.3f} MB")
        print(f"Parquet size: {parquet_bytes / 1e6:.3f} MB")

        # Warm up both formats; these runs are excluded.
        for fmt, path in [
            ("CSV", csv_path),
            ("Parquet", parquet_path),
        ]:
            _, matched, _ = run_query(path, fmt)
            print(f"{fmt} warm-up: {matched:,} matching records")

        # Alternate query order to reduce order bias.
        for run in range(1, 4):
            if run % 2 == 1:
                order = [
                    ("CSV", csv_path),
                    ("Parquet", parquet_path),
                ]
            else:
                order = [
                    ("Parquet", parquet_path),
                    ("CSV", csv_path),
                ]

            for fmt, path in order:
                elapsed, matched, memory_mb = run_query(path, fmt)

                rows.append({
                    "dataset_rows": size,
                    "format": fmt,
                    "run": run,
                    "execution_seconds": round(elapsed, 6),
                    "matched_records": matched,
                    "peak_process_tree_rss_mb": round(memory_mb, 2),
                    "file_size_mb": round(
                        (csv_bytes if fmt == "CSV" else parquet_bytes)
                        / 1e6, 3
                    ),
                    "parquet_conversion_seconds": round(
                        conversion_seconds, 3
                    ) if fmt == "Parquet" else "",
                })

                print(
                    f"{fmt} | Run {run} | "
                    f"Time: {elapsed:.3f} s | "
                    f"Peak process memory: {memory_mb:.1f} MB | "
                    f"Matches: {matched:,}"
                )

    # Save all individual measurements.
    raw_file = RESULTS / "rq3_csv_vs_parquet.csv"

    with raw_file.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=rows[0].keys()
        )
        writer.writeheader()
        writer.writerows(rows)

    # Save combined summary.
    summary = []

    for size in datasets:
        for fmt in ["CSV", "Parquet"]:
            group = [
                r for r in rows
                if r["dataset_rows"] == size
                and r["format"] == fmt
            ]

            summary.append({
                "dataset_rows": size,
                "format": fmt,
                "mean_execution_seconds": round(
                    sum(r["execution_seconds"] for r in group)
                    / len(group), 6
                ),
                "sd_execution_seconds": round(
                    pd.Series([
                        r["execution_seconds"] for r in group
                    ]).std(ddof=1), 6
                ),
                "mean_peak_process_tree_rss_mb": round(
                    sum(
                        r["peak_process_tree_rss_mb"]
                        for r in group
                    ) / len(group), 2
                ),
                "file_size_mb": group[0]["file_size_mb"],
                "matched_records": group[0]["matched_records"],
            })

    summary_file = RESULTS / "rq3_summary.csv"

    with summary_file.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=summary[0].keys()
        )
        writer.writeheader()
        writer.writerows(summary)

    print(f"\n{'=' * 55}")
    print("RQ3 EXPERIMENT COMPLETED")
    print(f"{'=' * 55}")

    for item in summary:
        print(item)

    print(f"\nRaw results: {raw_file}")
    print(f"Summary: {summary_file}")

    # Verify matching counts agree between formats.
    print("\nCorrectness checks:")
    for size in datasets:
        group = [
            r for r in rows
            if r["dataset_rows"] == size
        ]
        counts = {
            r["matched_records"] for r in group
        }
        print(
            f"{size:,} rows: "
            f"{'PASS' if len(counts) == 1 else 'CHECK COUNTS'}"
        )

finally:
    stop_monitor.set()
    spark.stop()
