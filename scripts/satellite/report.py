#!/usr/bin/env python3
"""Print the satellite ingestor's latency percentiles, lag and counters.

Reads the Prometheus text format from the ingestor's /metrics endpoint.
Percentiles are interpolated within histogram buckets, the same way
Prometheus histogram_quantile does.

Usage:
    python3 scripts/satellite/report.py                  # http://localhost:9091/metrics
    python3 scripts/satellite/report.py --watch 10       # repeat every 10 s
    python3 scripts/satellite/report.py http://host:9091/metrics
"""

import argparse
import re
import time
import urllib.request

LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)$')
LABEL = re.compile(r'(\w+)="([^"]*)"')


def scrape(url):
    with urllib.request.urlopen(url, timeout=5) as resp:
        text = resp.read().decode()
    samples = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = LINE.match(line)
        if m:
            labels = dict(LABEL.findall(m.group(2) or ""))
            samples.append((m.group(1), labels, float(m.group(3))))
    return samples


def quantile(q, buckets):
    """buckets: sorted list of (upper_bound, cumulative_count)."""
    total = buckets[-1][1]
    if total == 0:
        return None
    rank = q * total
    prev_bound, prev_count = 0.0, 0.0
    for bound, count in buckets:
        if count >= rank:
            if bound == float("inf"):
                return prev_bound
            span = count - prev_count
            frac = 0 if span == 0 else (rank - prev_count) / span
            return prev_bound + (bound - prev_bound) * frac
        prev_bound, prev_count = bound, count
    return prev_bound


def value(samples, name, **labels):
    for n, l, v in samples:
        if n == name and all(l.get(k) == want for k, want in labels.items()):
            return v
    return None


def report(url):
    s = scrape(url)
    buckets = sorted(
        (float("inf") if l["le"] == "+Inf" else float(l["le"]), v)
        for n, l, v in s
        if n == "satellite_ingest_latency_seconds_bucket"
    )
    count = value(s, "satellite_ingest_latency_seconds_count") or 0
    print(time.strftime("%H:%M:%S"), url)
    if buckets and count:
        for q in (0.5, 0.95, 0.99):
            print(f"  latency p{int(q * 100):<3} {quantile(q, buckets) * 1000:8.1f} ms")
        print(f"  messages      {int(count)}")
    else:
        print("  no messages committed yet")
    print(f"  next offset   {int(value(s, 'satellite_next_offset') or 0)}")
    print(f"  lag offsets   {int(value(s, 'satellite_feed_lag_offsets') or 0)}")
    lag_s = value(s, "satellite_event_time_lag_seconds")
    if lag_s is not None:
        print(f"  lag time      {lag_s:.0f} s of feed time")
    print(f"  outbox queue  {int(value(s, 'satellite_outbox_backlog') or 0)}")
    print(f"  dead letters  {int(value(s, 'satellite_dead_letters') or 0)}")
    for kind in ("late", "low_confidence", "invalid", "unknown_key", "duplicate"):
        v = value(s, "satellite_durable_count", kind=kind)
        print(f"  {kind:<13} {int(v or 0)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("url", nargs="?", default="http://localhost:9091/metrics")
    ap.add_argument("--watch", type=float, default=0, help="repeat every N seconds")
    args = ap.parse_args()
    while True:
        report(args.url)
        if not args.watch:
            break
        time.sleep(args.watch)
        print()


if __name__ == "__main__":
    main()
