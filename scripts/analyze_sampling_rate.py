"""
Analyze sampling rates for each data type from parsed files.

Usage:
  python scripts/analyze_sampling_rate.py
  python scripts/analyze_sampling_rate.py --input ./downloads/parsed
  python scripts/analyze_sampling_rate.py --uid abc123

Calculates sampling rate based on timestamp differences between consecutive records.
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean, median, stdev


def parse_timestamp(ts_str: str) -> float:
    """Parse timestamp_epoch_ms to float."""
    try:
        return float(ts_str)
    except (ValueError, TypeError):
        return None


def analyze_file(file_path: Path) -> dict:
    """
    Analyze a single CSV file and return sampling statistics.

    Returns:
        dict with count, mean_interval_ms, median_interval_ms, estimated_hz
    """
    timestamps = []

    with file_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = parse_timestamp(row.get("timestamp_epoch_ms", ""))
            if ts is not None:
                timestamps.append(ts)

    if len(timestamps) < 2:
        return {
            "count": len(timestamps),
            "mean_interval_ms": None,
            "median_interval_ms": None,
            "stdev_interval_ms": None,
            "estimated_hz": None,
        }

    # Sort timestamps and calculate intervals
    timestamps.sort()
    intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps) - 1)]

    # Filter out negative or zero intervals (shouldn't happen but just in case)
    intervals = [i for i in intervals if i > 0]

    if not intervals:
        return {
            "count": len(timestamps),
            "mean_interval_ms": None,
            "median_interval_ms": None,
            "stdev_interval_ms": None,
            "estimated_hz": None,
        }

    mean_interval = mean(intervals)
    median_interval = median(intervals)
    stdev_interval = stdev(intervals) if len(intervals) > 1 else 0

    # Estimate Hz from mean interval
    estimated_hz = 1000.0 / mean_interval if mean_interval > 0 else None

    return {
        "count": len(timestamps),
        "mean_interval_ms": mean_interval,
        "median_interval_ms": median_interval,
        "stdev_interval_ms": stdev_interval,
        "estimated_hz": estimated_hz,
    }


def find_parsed_files(input_dir: Path, uid_filter: str = None) -> dict:
    """
    Find all parsed CSV files grouped by type_tag.

    Returns:
        dict mapping type_tag -> list of file paths
    """
    files_by_type = defaultdict(list)

    # Structure: parsed/$uid/$type_tag/$yyyymmdd.csv
    parsed_dir = input_dir
    if not parsed_dir.exists():
        return files_by_type

    for uid_dir in parsed_dir.iterdir():
        if not uid_dir.is_dir():
            continue
        if uid_filter and uid_dir.name != uid_filter:
            continue

        for type_dir in uid_dir.iterdir():
            if not type_dir.is_dir():
                continue
            type_tag = type_dir.name

            for csv_file in type_dir.glob("*.csv"):
                files_by_type[type_tag].append(csv_file)

    return files_by_type


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze sampling rates for each data type from parsed files."
    )
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=Path("./downloads/parsed"),
        help="Input directory containing parsed files (default: ./downloads/parsed)",
    )
    parser.add_argument(
        "--uid",
        type=str,
        default=None,
        help="Filter by specific UID",
    )
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: Input directory not found: {args.input}", file=sys.stderr)
        return 1

    print(f"Scanning for parsed files in: {args.input}")
    files_by_type = find_parsed_files(args.input, args.uid)

    if not files_by_type:
        print("No parsed files found.")
        return 0

    print(f"Found {sum(len(f) for f in files_by_type.values())} files across {len(files_by_type)} data types.\n")

    # Analyze each type
    results = {}
    for type_tag in sorted(files_by_type.keys()):
        files = files_by_type[type_tag]

        # Aggregate stats across all files for this type
        all_intervals = []
        total_count = 0

        for file_path in files:
            stats = analyze_file(file_path)
            total_count += stats["count"]

            # Re-read to get all intervals for aggregate stats
            timestamps = []
            with file_path.open(newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ts = parse_timestamp(row.get("timestamp_epoch_ms", ""))
                    if ts is not None:
                        timestamps.append(ts)

            if len(timestamps) >= 2:
                timestamps.sort()
                intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps) - 1)]
                all_intervals.extend([i for i in intervals if i > 0])

        if all_intervals:
            mean_interval = mean(all_intervals)
            median_interval = median(all_intervals)
            stdev_interval = stdev(all_intervals) if len(all_intervals) > 1 else 0
            estimated_hz = 1000.0 / mean_interval if mean_interval > 0 else None
        else:
            mean_interval = None
            median_interval = None
            stdev_interval = None
            estimated_hz = None

        results[type_tag] = {
            "files": len(files),
            "total_samples": total_count,
            "mean_interval_ms": mean_interval,
            "median_interval_ms": median_interval,
            "stdev_interval_ms": stdev_interval,
            "estimated_hz": estimated_hz,
        }

    # Print results
    print("=" * 80)
    print(f"{'Type Tag':<15} {'Files':<8} {'Samples':<12} {'Mean (ms)':<12} {'Median (ms)':<12} {'StdDev (ms)':<12} {'Est. Hz':<10}")
    print("=" * 80)

    for type_tag, stats in sorted(results.items()):
        mean_str = f"{stats['mean_interval_ms']:.2f}" if stats['mean_interval_ms'] else "N/A"
        median_str = f"{stats['median_interval_ms']:.2f}" if stats['median_interval_ms'] else "N/A"
        stdev_str = f"{stats['stdev_interval_ms']:.2f}" if stats['stdev_interval_ms'] else "N/A"
        hz_str = f"{stats['estimated_hz']:.2f}" if stats['estimated_hz'] else "N/A"

        print(f"{type_tag:<15} {stats['files']:<8} {stats['total_samples']:<12} {mean_str:<12} {median_str:<12} {stdev_str:<12} {hz_str:<10}")

    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
