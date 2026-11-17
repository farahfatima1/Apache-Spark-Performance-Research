
import os
import sys
import time
import subprocess
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType
)

BASE = r"C:\SparkResearch"
CSV_PATH = os.path.join(BASE, "data", "events_1000000.csv")
RESULTS_DIR = os.path.join(BASE, "results")

OUTPUT = os.path.join(RESULTS_DIR, "rq1_parallelism_1m.csv")
SUMMARY = os.path.join(RESULTS_DIR, "rq1_summary_1m.csv")

REPETITIONS = 3
DATASET_ROWS = 1_000_000
CORES = ["1", "2", "4", "*"]

SCHEMA = StructType([
    StructField("record_id", LongType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("bytes_transferred", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("message", StringType(), True),
])


def run_configuration(core):
    master = "local[*]" if core == "*" else f"local[{core}]"

    spark = (
        SparkSession.builder
        .master(master)
        .appName(f"RQ1_1m_{core}")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    records = []

    try:
        def execute_query():
            df = (
                spark.read
                .option("header", True)
                .schema(SCHEMA)
                .csv(CSV_PATH)
            )

            return df.filter(
                (F.col("event_type") == "alert") &
                (F.col("status") == "failure")
            ).count()

        print(f"\nCPU configuration: local[{core}]")

        # Warm-up run, excluded from measured results.
        expected_count = execute_query()
        print(f"Warm-up completed. Matching records: {expected_count}")

        for repetition in range(1, REPETITIONS + 1):
            start = time.perf_counter()
            match_count = execute_query()
            elapsed = time.perf_counter() - start

            records.append({
                "cores": core,
                "master": master,
                "dataset_rows": DATASET_ROWS,
                "repetition": repetition,
                "execution_seconds": elapsed,
                "throughput_rows_per_second":
                    DATASET_ROWS / elapsed if elapsed > 0 else None,
                "matching_records": match_count
            })

            print(
                f"Run {repetition}/{REPETITIONS}: "
                f"{elapsed:.3f} seconds; "
                f"throughput = {DATASET_ROWS / elapsed:.0f} rows/sec"
            )

        return records

    finally:
        spark.stop()


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)

    if not os.path.isfile(CSV_PATH):
        raise FileNotFoundError(f"Dataset not found: {CSV_PATH}")

    all_records = []

    for core in CORES:
        print(f"\nStarting separate Spark process for local[{core}]")

        subprocess.run(
            [sys.executable, os.path.abspath(__file__), "--worker", core],
            check=True
        )

        part_file = os.path.join(
            RESULTS_DIR, f"rq1_1m_cores_{core.replace('*', 'all')}.csv"
        )

        if not os.path.isfile(part_file):
            raise FileNotFoundError(
                f"Worker results not found: {part_file}"
            )

        all_records.append(pd.read_csv(part_file))

    combined = pd.concat(all_records, ignore_index=True)
    combined.to_csv(OUTPUT, index=False)

    summary = (
        combined.groupby("cores")
        .agg(
            mean_seconds=("execution_seconds", "mean"),
            sd_seconds=("execution_seconds", "std"),
            mean_throughput=("throughput_rows_per_second", "mean")
        )
        .reset_index()
    )

    summary.to_csv(SUMMARY, index=False)

    print("\nRQ1 1,000,000-row experiment completed.")
    print(f"Raw results: {OUTPUT}")
    print(f"Summary: {SUMMARY}")
    print("\nMeasured summary:")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--worker":
        core = sys.argv[2]
        records = run_configuration(core)

        part_file = os.path.join(
            RESULTS_DIR, f"rq1_1m_cores_{core.replace('*', 'all')}.csv"
        )
        pd.DataFrame(records).to_csv(part_file, index=False)
    else:
        main()
