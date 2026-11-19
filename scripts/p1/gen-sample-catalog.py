#!/usr/bin/env python3
"""Generate a 50,000-row sample historical earthquake catalog skewed to Sulawesi Tengah."""

import argparse
import csv
import datetime as dt
import os
import random


REGIONS = [
    # (name, lat_min, lat_max, lon_min, lon_max, weight)
    ("Sulawesi Tengah", -2.2, 0.8, 119.3, 122.8, 0.80),
    ("Jawa Barat", -7.9, -6.4, 106.2, 108.8, 0.07),
    ("Aceh", 2.5, 5.8, 95.1, 97.9, 0.05),
    ("Maluku Utara", -1.5, 2.5, 127.0, 129.0, 0.04),
    ("Papua", -4.5, -1.0, 134.0, 140.5, 0.04),
]


def pick_region(rng: random.Random):
    r = rng.random()
    acc = 0.0
    for item in REGIONS:
        acc += item[5]
        if r <= acc:
            return item
    return REGIONS[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate skewed historical earthquake catalog CSV")
    parser.add_argument("--rows", type=int, default=50000, help="Number of rows to generate (default: 50000)")
    parser.add_argument("--out", default="data/sample-catalog-50k.csv", help="Output CSV path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    rng = random.Random(args.seed)
    base_time = dt.datetime(2015, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["time", "latitude", "longitude", "depth", "mag", "place", "id", "network"])
        for i in range(args.rows):
            name, lat_min, lat_max, lon_min, lon_max, _ = pick_region(rng)
            ts = base_time + dt.timedelta(minutes=i * 3, seconds=rng.randint(0, 59))
            lat = round(rng.uniform(lat_min, lat_max), 4)
            lon = round(rng.uniform(lon_min, lon_max), 4)
            depth = round(rng.uniform(5.0, 150.0), 1)
            mag = round(rng.triangular(2.5, 7.8, 4.3), 1)
            ev_id = f"hist-eq-{i + 1:06d}"
            writer.writerow([ts.strftime("%Y-%m-%dT%H:%M:%SZ"), lat, lon, depth, mag, name, ev_id, "BMKG-HIST"])

    print(f"Generated {args.rows} rows -> {args.out}")


if __name__ == "__main__":
    main()
