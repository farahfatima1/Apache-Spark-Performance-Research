
import csv
import json
import statistics
import subprocess
import sys
from pathlib import Path

BASE = Path(r"C:\SparkResearch")
CHILD = BASE / "rq_memory_unified_child.py"
OUT = BASE / "results" / "rq_memory_unified.csv"

PYTHON = sys.executable
SIZES = [100000, 500000, 1000000]
FORMATS = ["CSV", "PARQUET"]
RUNS = 3

results = []

for size in SIZES:
    for run in range(1, RUNS + 1):
        # Alternate format order across runs.
        order = FORMATS if run % 2 == 1 else list(reversed(FORMATS))

        for fmt in order:
            print(f"\nDataset={size}, Format={fmt}, Run={run}")

            proc = subprocess.run(
                [PYTHON, str(CHILD), str(size), fmt, str(run)],
                cwd=str(BASE),
                capture_output=True,
                text=True
            )

            if proc.returncode != 0:
                print("Child process failed:")
                print(proc.stdout)
                print(proc.stderr)
                sys.exit(proc.returncode)

            json_lines = [
                line for line in proc.stdout.splitlines()
                if line.strip().startswith("{")
            ]
            if not json_lines:
                print("No JSON result found:")
                print(proc.stdout)
                print(proc.stderr)
                sys.exit(1)

            record = json.loads(json_lines[-1])
            expected = {100000: 6751, 500000: 33106, 1000000: 66981}

            if record["matched_records"] != expected[size]:
                raise RuntimeError(f"Incorrect count: {record}")

            results.append(record)
            print(record)

OUT.parent.mkdir(parents=True, exist_ok=True)

with OUT.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)

print("\n=== UNIFIED MEMORY SUMMARY ===")
for size in SIZES:
    for fmt in FORMATS:
        group = [
            r for r in results
            if r["dataset_rows"] == size and r["format"] == fmt
        ]
        rss = [r["peak_process_tree_rss_mb"] for r in group]
        times = [r["execution_seconds"] for r in group]

        print(
            f"{size:>7} | {fmt:<7} | "
            f"RSS mean={statistics.mean(rss):.2f} MB, "
            f"RSS SD={statistics.stdev(rss):.2f} MB | "
            f"time mean={statistics.mean(times):.3f} s"
        )

print(f"\nSaved raw results: {OUT}")
print(f"Successful measured trials: {len(results)}")
