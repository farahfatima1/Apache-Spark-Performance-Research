
import csv
import time
import threading
from pathlib import Path

import psutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BASE = Path(r"C:\SparkResearch")
DATA = BASE / "data"
RESULTS = BASE / "results"
RESULTS.mkdir(exist_ok=True)

datasets = {
    100_000: DATA / "events_100000.csv",
    500_000: DATA / "events_500000.csv",
    1_000_000: DATA / "events_1000000.csv",
}

spark = (
    SparkSession.builder
    .appName("RQ2_Dataset_Size")
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


rows = []

try:
    for size, path in datasets.items():
        if not path.exists():
            raise FileNotFoundError(f"Dataset not found: {path}")

        print(f"\nTesting {size:,} rows")

        # One warm-up run, excluded from measurements
        df = spark.read.option("header", True).option(
            "inferSchema", True
        ).csv(str(path))

        df.filter(
            (col("event_type") == "alert") &
            (col("status") == "failure")
        ).count()

        del df

        for run in range(1, 4):
            peak_mb[0] = 0.0
            stop_monitor.clear()

            monitor = threading.Thread(
                target=monitor_memory, daemon=True
            )
            monitor.start()

            start = time.perf_counter()

            df = spark.read.option("header", True).option(
                "inferSchema", True
            ).csv(str(path))

            matched = df.filter(
                (col("event_type") == "alert") &
                (col("status") == "failure")
            ).count()

            elapsed = time.perf_counter() - start

            stop_monitor.set()
            monitor.join()
            del df

            throughput = size / elapsed

            result = {
                "dataset_rows": size,
                "run": run,
                "cpu_configuration": "local[2]",
                "execution_seconds": round(elapsed, 6),
                "throughput_rows_per_second": round(throughput, 2),
                "matched_records": matched,
                "peak_process_tree_rss_mb": round(peak_mb[0], 2),
            }
            rows.append(result)

            print(
                f"Run {run}: {elapsed:.3f}s | "
                f"{throughput:,.0f} rows/s | "
                f"Matched: {matched:,} | "
                f"Peak process memory: {peak_mb[0]:.1f} MB"
            )

    raw_file = RESULTS / "rq2_dataset_size.csv"
    with raw_file.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    summary_file = RESULTS / "rq2_summary.csv"
    summary = []

    for size in datasets:
        group = [r for r in rows if r["dataset_rows"] == size]
        times = [r["execution_seconds"] for r in group]

        summary.append({
            "dataset_rows": size,
            "mean_seconds": round(sum(times) / len(times), 6),
            "mean_throughput_rows_per_second": round(
                sum(r["throughput_rows_per_second"] for r in group)
                / len(group), 2
            ),
            "mean_peak_process_tree_rss_mb": round(
                sum(r["peak_process_tree_rss_mb"] for r in group)
                / len(group), 2
            ),
            "matched_records": group[0]["matched_records"],
        })

    with summary_file.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary[0].keys())
        writer.writeheader()
        writer.writerows(summary)

    print("\nRQ2 experiment completed.")
    print("Saved:", raw_file)
    print("Saved:", summary_file)
    print("\nSummary:")
    for item in summary:
        print(item)

finally:
    stop_monitor.set()
    spark.stop()
