
import os
import sys
import json
import time
import platform
import subprocess
import pandas as pd
import psutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)

# ============================================================
# RQ4: Joint effect of CPU parallelism and data format
# ============================================================

BASE_DIR = r"C:\SparkResearch"
DATA_DIR = os.path.join(BASE_DIR, "data")
PARQUET_DIR = os.path.join(DATA_DIR, "parquet")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

os.makedirs(RESULTS_DIR, exist_ok=True)

DATASETS = {
    100_000: (
        os.path.join(DATA_DIR, "events_100000.csv"),
        os.path.join(PARQUET_DIR, "events_100000.parquet"),
    ),
    500_000: (
        os.path.join(DATA_DIR, "events_500000.csv"),
        os.path.join(PARQUET_DIR, "events_500000.parquet"),
    ),
    1_000_000: (
        os.path.join(DATA_DIR, "events_1000000.csv"),
        os.path.join(PARQUET_DIR, "events_1000000.parquet"),
    ),
}

CPU_CONFIGS = ["local[1]", "local[2]", "local[4]", "local[*]"]
FORMATS = ["CSV", "Parquet"]
REPETITIONS = 3

# Replace this with the actual seed used when generating the data.
# Leave as None if the seed was not recorded; do not guess it.
DATA_GENERATION_SEED = None

# Explicit schema prevents CSV schema inference overhead.
SCHEMA = StructType([
    StructField("record_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("duration_ms", LongType(), True),
    StructField("bytes_transferred", LongType(), True),
    StructField("status", StringType(), True),
    StructField("message", StringType(), True),
])


def process_tree_rss_mb():
    """
    Sample RSS of this Python process and accessible descendants.
    This is a post-query sample, NOT peak memory usage.
    """
    proc = psutil.Process()
    processes = [proc]

    try:
        processes.extend(proc.children(recursive=True))
    except (psutil.Error, OSError):
        pass

    total_bytes = 0

    for process in processes:
        try:
            total_bytes += process.memory_info().rss
        except (psutil.Error, OSError):
            pass

    return total_bytes / (1024 * 1024)


def read_dataset(spark, size, data_format):
    csv_path, parquet_path = DATASETS[size]

    if data_format == "CSV":
        return (
            spark.read
            .option("header", True)
            .schema(SCHEMA)
            .csv(csv_path)
        )

    return spark.read.parquet(parquet_path)


def run_query(spark, size, data_format):
    """
    Create a new DataFrame read and run the filter/count query.
    Explicitly clear Spark SQL's cache before each execution.
    This does not clear operating-system filesystem caches.
    """
    spark.catalog.clearCache()

    df = read_dataset(spark, size, data_format)

    return (
        df.filter(
            (col("event_type") == "alert")
            & (col("status") == "failure")
        )
        .count()
    )


def get_java_version():
    """Return the installed Java version when the command is available."""
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        output = (result.stderr or result.stdout).strip()
        return output.splitlines()[0] if output else "Unknown"
    except (OSError, subprocess.SubprocessError):
        return "Unavailable"


def validate_input_files():
    """Fail early if any expected input file is missing."""
    missing = []

    for size, (csv_path, parquet_path) in DATASETS.items():
        if not os.path.isfile(csv_path):
            missing.append(csv_path)

        if not os.path.exists(parquet_path):
            missing.append(parquet_path)

    if missing:
        message = "Missing input files:\n" + "\n".join(missing)
        raise FileNotFoundError(message)


# ============================================================
# Environment metadata
# ============================================================

metadata = {
    "experiment": "RQ4_CPU_Parallelism_and_Data_Format",
    "operating_system": platform.platform(),
    "python_version": sys.version,
    "pandas_version": pd.__version__,
    "psutil_version": psutil.__version__,
    "java_version": get_java_version(),
    "dataset_generation_seed": (
        DATA_GENERATION_SEED
        if DATA_GENERATION_SEED is not None
        else "Not recorded in this script"
    ),
    "cpu_configurations": CPU_CONFIGS,
    "formats": FORMATS,
    "dataset_sizes": list(DATASETS.keys()),
    "warmup_runs_per_configuration": 1,
    "measured_runs_per_configuration": REPETITIONS,
    "csv_schema": "Explicit predefined schema",
    "parquet_schema": "Stored Parquet schema",
    "memory_measurement": (
        "Process-tree RSS sampled after each timed query; "
        "not peak memory"
    ),
    "spark_cache": (
        "Spark SQL cache cleared before each query; "
        "OS filesystem cache not controlled"
    ),
    "spark_session_policy": (
        "One Spark session reused per CPU configuration"
    ),
    "format_order": (
        "CSV then Parquet for 100K and 1M; "
        "Parquet then CSV for 500K"
    ),
}

metadata_path = os.path.join(RESULTS_DIR, "rq4_environment.json")

# ============================================================
# Validate files and run the experiment
# ============================================================

validate_input_files()

records = []
spark_version = None

for cpu in CPU_CONFIGS:
    print(f"\n===== CPU configuration: {cpu} =====")

    spark = (
        SparkSession.builder
        .appName(f"RQ4_{cpu}")
        .master(cpu)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    if spark_version is None:
        spark_version = spark.version

    try:
        for size in DATASETS:
            # Alternate the format order to reduce systematic
            # first-format / second-format order effects.
            if size in (100_000, 1_000_000):
                formats_this_size = FORMATS.copy()
            else:
                formats_this_size = list(reversed(FORMATS))

            for fmt in formats_this_size:
                print(f"\nTesting {size:,} rows | {fmt}")

                # One warm-up execution, excluded from measurements.
                expected_count = run_query(spark, size, fmt)

                for rep in range(1, REPETITIONS + 1):
                    start = time.perf_counter()

                    count = run_query(spark, size, fmt)

                    elapsed = time.perf_counter() - start

                    # RSS is sampled after the timed query completes.
                    rss_mb = process_tree_rss_mb()

                    if count != expected_count:
                        raise ValueError(
                            f"Count mismatch: CPU={cpu}, "
                            f"size={size}, format={fmt}, "
                            f"expected={expected_count}, got={count}"
                        )

                    records.append({
                        "cpu_config": cpu,
                        "dataset_size": size,
                        "format": fmt,
                        "run": rep,
                        "execution_time_s": elapsed,
                        "throughput_rows_s": size / elapsed,
                        "process_tree_rss_mb": rss_mb,
                        "matching_records": count,
                    })

                    print(
                        f"Run {rep}: "
                        f"{elapsed:.3f} s | "
                        f"{size / elapsed:,.0f} rows/s | "
                        f"sampled RSS={rss_mb:.1f} MB | "
                        f"matches={count:,}"
                    )

    finally:
        spark.stop()

# Add the actual Spark version to the metadata.
metadata["spark_version"] = spark_version

with open(metadata_path, "w", encoding="utf-8") as file:
    json.dump(metadata, file, indent=2)

# ============================================================
# Save raw results and summary
# ============================================================

raw = pd.DataFrame(records)

raw_path = os.path.join(RESULTS_DIR, "rq4_cpu_format.csv")
summary_path = os.path.join(RESULTS_DIR, "rq4_summary.csv")

raw.to_csv(raw_path, index=False)

summary = (
    raw.groupby(["cpu_config", "dataset_size", "format"])
    .agg(
        mean_query_time_s=("execution_time_s", "mean"),
        sd_query_time_s=("execution_time_s", "std"),
        mean_throughput_rows_s=("throughput_rows_s", "mean"),
        mean_process_tree_rss_mb=("process_tree_rss_mb", "mean"),
        matching_records=("matching_records", "first"),
        repetitions=("run", "count"),
    )
    .reset_index()
)

summary.to_csv(summary_path, index=False)

print("\n===== RQ4 SUMMARY =====")

# Display rounding only; CSV files retain full precision.
display_summary = summary.copy()

display_summary["mean_query_time_s"] = (
    display_summary["mean_query_time_s"].round(3)
)
display_summary["sd_query_time_s"] = (
    display_summary["sd_query_time_s"].round(3)
)
display_summary["mean_throughput_rows_s"] = (
    display_summary["mean_throughput_rows_s"].round(0).astype("int64")
)
display_summary["mean_process_tree_rss_mb"] = (
    display_summary["mean_process_tree_rss_mb"].round(2)
)

print(display_summary.to_string(index=False))

print("\nSaved files:")
print(f"Raw results: {raw_path}")
print(f"Summary:     {summary_path}")
print(f"Metadata:    {metadata_path}")
