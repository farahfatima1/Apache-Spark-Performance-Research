
import sys
import time
import json
import threading

import psutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

CSV_PATH = r"C:\SparkResearch\data\events_100000.csv"
EXPECTED_MATCHES = 6751


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


def timed_query(spark, proc):
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
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(CSV_PATH)
        )
        count = df.filter(
            (col("event_type") == "alert") &
            (col("status") == "failure")
        ).count()
        elapsed = time.perf_counter() - start
    finally:
        stop.set()
        monitor.join()

    return elapsed, peak[0], count


if __name__ == "__main__":
    run_id = int(sys.argv[1])

    spark = (
        SparkSession.builder
        .appName("RQ2_Memory_Recheck")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    proc = psutil.Process()

    try:
        # Warm-up query, excluded from results
        _, _, warmup_count = timed_query(spark, proc)
        if warmup_count != EXPECTED_MATCHES:
            raise RuntimeError(f"Warm-up count was {warmup_count}")

        # Measured query
        elapsed, rss, count = timed_query(spark, proc)
        if count != EXPECTED_MATCHES:
            raise RuntimeError(f"Measured count was {count}")

        print(json.dumps({
            "run": run_id,
            "seconds": round(elapsed, 6),
            "peak_rss_mb": round(rss, 2),
            "matched": count
        }))
    finally:
        spark.stop()
