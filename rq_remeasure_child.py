
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
CPU = sys.argv[3]
TRIAL = int(sys.argv[4])

EXPECTED = {
    100000: 6751,
    500000: 33106,
    1000000: 66981,
}

if SIZE not in EXPECTED:
    raise ValueError(f"Unsupported dataset size: {SIZE}")
if FMT not in ("CSV", "PARQUET"):
    raise ValueError(f"Unsupported format: {FMT}")
if CPU not in ("local[1]", "local[2]", "local[4]", "local[*]"):
    raise ValueError(f"Unsupported CPU configuration: {CPU}")

if FMT == "CSV":
    PATH = DATA / f"events_{SIZE}.csv"
else:
    PATH = DATA / "parquet" / f"events_{SIZE}.parquet"

if not PATH.is_file():
    raise FileNotFoundError(f"Dataset not found: {PATH}")

spark = (
    SparkSession.builder
    .appName(f"RQ1_RQ4_{SIZE}_{FMT}_{CPU}_{TRIAL}")
    .master(CPU)
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
proc = psutil.Process()


def process_tree_rss_mb(proc):
    total = 0
    try:
        processes = [proc] + proc.children(recursive=True)
        for p in processes:
            try:
                if p.is_running():
                    total += p.memory_info().rss
            except (psutil.Error, OSError):
                pass
    except (psutil.Error, OSError):
        pass
    return total / (1024 * 1024)


def timed_query():
    peak = [process_tree_rss_mb(proc)]
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            peak[0] = max(peak[0], process_tree_rss_mb(proc))
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

    return elapsed, peak[0], matched


try:
    # Discarded warm-up query in this fresh Spark process.
    _, _, warmup_count = timed_query()
    if warmup_count != EXPECTED[SIZE]:
        raise RuntimeError(
            f"Unexpected warm-up count: {warmup_count}; "
            f"expected {EXPECTED[SIZE]}"
        )

    # One measured query using the same filter and count.
    seconds, peak_rss_mb, matched = timed_query()

    if matched != EXPECTED[SIZE]:
        raise RuntimeError(
            f"Unexpected measured count: {matched}; "
            f"expected {EXPECTED[SIZE]}"
        )

    print(json.dumps({
        "format": FMT,
        "rows": SIZE,
        "cpu": CPU,
        "trial": TRIAL,
        "seconds": round(seconds, 6),
        "peak_rss_mb": round(peak_rss_mb, 2),
        "matched": matched
    }))
finally:
    spark.stop()
