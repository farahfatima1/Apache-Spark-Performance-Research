
import os
import random
import string
import time

import pandas as pd

BASE_DIR = r"C:\SparkResearch"
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

def generate_dataset(rows):
    rng = random.Random(42 + rows)
    categories = ["login", "network", "file", "process", "alert"]
    statuses = ["success", "failure", "warning"]

    data = {
        "record_id": range(1, rows + 1),
        "user_id": [rng.randint(1, 10000) for _ in range(rows)],
        "event_type": [rng.choice(categories) for _ in range(rows)],
        "duration_ms": [rng.randint(1, 5000) for _ in range(rows)],
        "bytes_transferred": [
            rng.randint(100, 1_000_000) for _ in range(rows)
        ],
        "status": [rng.choice(statuses) for _ in range(rows)],
        "message": [
            "".join(rng.choices(string.ascii_lowercase, k=20))
            for _ in range(rows)
        ],
    }

    return pd.DataFrame(data)


if __name__ == "__main__":
    for rows in [100_000, 500_000, 1_000_000]:
        output_path = os.path.join(DATA_DIR, f"events_{rows}.csv")

        if os.path.exists(output_path):
            print(f"Already exists; skipping: {output_path}")
            continue

        print(f"\nGenerating {rows:,} rows...")
        start = time.perf_counter()

        df = generate_dataset(rows)
        df.to_csv(output_path, index=False)

        elapsed = time.perf_counter() - start
        size_mb = os.path.getsize(output_path) / (1024 * 1024)

        print(f"Saved: {output_path}")
        print(f"Rows: {len(df):,}")
        print(f"CSV size: {size_mb:.2f} MB")
        print(f"Generation and writing time: {elapsed:.2f} seconds")

        del df

    print("\nDataset generation finished.")
