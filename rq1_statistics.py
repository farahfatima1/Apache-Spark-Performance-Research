
import csv
import math
import statistics
from pathlib import Path

ROOT = Path(r"C:\SparkResearch\results")
T_CRITICAL = 4.30265273  # 95% CI, df = 2, for 3 runs

files = {
    "100k": {
        "local[1]": "rq1_cores_1.csv",
        "local[2]": "rq1_cores_2.csv",
        "local[4]": "rq1_cores_4.csv",
        "local[*]": "rq1_cores_all.csv",
    },
    "500k": {
        "local[1]": "rq1_500k_cores_1.csv",
        "local[2]": "rq1_500k_cores_2.csv",
        "local[4]": "rq1_500k_cores_4.csv",
        "local[*]": "rq1_500k_cores_all.csv",
    },
    "1M": {
        "local[1]": "rq1_1m_cores_1.csv",
        "local[2]": "rq1_1m_cores_2.csv",
        "local[4]": "rq1_1m_cores_4.csv",
        "local[*]": "rq1_1m_cores_all.csv",
    },
}

output = []

for dataset, configs in files.items():
    for config, filename in configs.items():
        path = ROOT / filename

        if not path.exists():
            print(f"MISSING FILE: {path}")
            continue

        with path.open(newline="") as f:
            rows = list(csv.DictReader(f))

        times = sorted(
            float(row["execution_seconds"])
            for row in rows
        )

        if len(times) != 3:
            print(
                f"WARNING: {filename} contains "
                f"{len(times)} runs, not 3."
            )
            continue

        mean = statistics.mean(times)
        sd = statistics.stdev(times)
        margin = T_CRITICAL * sd / math.sqrt(len(times))
        low = mean - margin
        high = mean + margin
        cv = (sd / mean * 100) if mean else 0

        result = {
            "Dataset": dataset,
            "Configuration": config,
            "Run 1 (s)": times[0],
            "Run 2 (s)": times[1],
            "Run 3 (s)": times[2],
            "Mean (s)": mean,
            "SD (s)": sd,
            "Min (s)": min(times),
            "Max (s)": max(times),
            "CV (%)": cv,
            "95% CI lower (s)": low,
            "95% CI upper (s)": high,
        }
        output.append(result)

out_path = ROOT / "rq1_complete_statistics.csv"

if output:
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=output[0].keys()
        )
        writer.writeheader()
        writer.writerows(output)

    print("\nRQ1 COMPLETE STATISTICS\n")

    for r in output:
        print(
            f"{r['Dataset']:>4} | "
            f"{r['Configuration']:<9} | "
            f"Runs: {r['Run 1 (s)']:.4f}, "
            f"{r['Run 2 (s)']:.4f}, "
            f"{r['Run 3 (s)']:.4f} | "
            f"Mean={r['Mean (s)']:.4f} | "
            f"SD={r['SD (s)']:.4f} | "
            f"Range={r['Min (s)']:.4f}-"
            f"{r['Max (s)']:.4f} | "
            f"CV={r['CV (%)']:.2f}% | "
            f"95% CI=[{r['95% CI lower (s)']:.4f}, "
            f"{r['95% CI upper (s)']:.4f}]"
        )

    print(f"\nSaved table: {out_path}")
else:
    print("No results were generated. Check filenames.")
