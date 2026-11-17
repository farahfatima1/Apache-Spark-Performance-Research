
import sys
import time
import json
import threading
from pathlib import Path

import psutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BASE = Path(r"C:\SparkResearch")
DATA = BASE / "data"
SIZE = int(sys.argv[1])
FMT = sys.argv[2].upper()
RUN = int(sys.argv[3])

if FMT not in ("CSV", "PARQUET"):
    raise ValueError("Format must be CSV or PARQUET")

CSV_PATH = DATA / f"events_{SIZE}.csv"
PARQUET_PATH = DATA / "parquet" / f"events_{SIZE}.parquet"
PATH = CSV_PATH if FMT == "CSV" else PARQUET_PATH

EXPECTED = {
    100000: 6751,
    500000: 33106,
    1000000: 66981,
}

if not PATH.exists():
    raise FileNotFoundError(f"Dataset not found: {PATH}")

spark = (
    SparkSession.builder
    .appName(f"UnifiedMemory_{SIZE}_{FMT}_{RUN}")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
process = psutil.Process()


def process_tree_rss_mb():
    total = 0
    try:
        processes = [process] + process.children(recursive=True)
        for p in processes:
            try:
                if p.is_running():
                    total += p.memory_info().rss
            except (psutil.Error, OSError):
                pass
    except (psutil.Error, OSError):
        pass
    return total / (1024 * 1024)


def query(measure_memory):
    peak = [process_tree_rss_mb()]
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            peak[0] = max(peak[0], process_tree_rss_mb())
            stop.wait(0.05)

    monitor = threading.Thread(target=sampler, daemon=True)
    monitor.start()

    try:
        start = time.perf_counter()

        if FMT == "CSV":
            df = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(str(PATH))
            )
        else:
            df = spark.read.parquet(str(PATH))

        matched = df.filter(
            (col("event_type") == "alert") &
            (col("status") == "failure")
        ).count()

        elapsed = time.perf_counter() - start
    finally:
        stop.set()
        monitor.join()

    del df
    return elapsed, peak[0], matched


try:
    # Warm-up query in this fresh JVM; exclude from results.
    _, _, warmup_count = query(False)
    if warmup_count != EXPECTED[SIZE]:
        raise RuntimeError(f"Unexpected warm-up count: {warmup_count}")

    # Measured query with the same sampling protocol.
    seconds, rss_mb, matched = query(True)
    if matched != EXPECTED[SIZE]:
        raise RuntimeError(f"Unexpected measured count: {matched}")

    print(json.dumps({
        "dataset_rows": SIZE,
        "format": FMT,
        "run": RUN,
        "execution_seconds": round(seconds, 6),
        "matched_records": matched,
        "peak_process_tree_rss_mb": round(rss_mb, 2)
    }))
finally:
    spark.stop()
