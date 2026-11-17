
import subprocess
import sys
import json
import statistics
from pathlib import Path

BASE = Path(r"C:\SparkResearch")
CHILD = BASE / "rq_recheck_child.py"
PYTHON = sys.executable
RUNS = 3

results = []

for i in range(1, RUNS + 1):
    print(f"\nStarting fresh process {i}/{RUNS}...")

    out = subprocess.run(
        [PYTHON, str(CHILD), str(i)],
        cwd=str(BASE),
        capture_output=True,
        text=True
    )

    if out.returncode != 0:
        print("Child process failed:")
        print(out.stdout)
        print(out.stderr)
        sys.exit(out.returncode)

    print(out.stdout)
    json_lines = [
        line for line in out.stdout.splitlines()
        if line.strip().startswith("{")
    ]
    if not json_lines:
        print("Could not find JSON result.")
        print(out.stderr)
        sys.exit(1)

    rec = json.loads(json_lines[-1])

    if rec["matched"] != 6751:
        raise RuntimeError(f"Unexpected match count: {rec['matched']}")

    results.append(rec)

rss = [r["peak_rss_mb"] for r in results]

print("\n=== Three measured runs ===")
print(f"Mean peak process-tree RSS: {statistics.mean(rss):.2f} MB")
print(f"Sample SD: {statistics.stdev(rss):.2f} MB")
print("All runs matched 6,751 records.")
