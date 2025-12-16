"""
Usage:
  python parse_payload.py
  python parse_payload.py input.csv -o parsed.csv
  python parse_payload.py input.csv -o parsed.jsonl --format jsonl
"""

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, TextIO


_INT_PATTERN = re.compile(r"^[+-]?\d+$")
_FLOAT_PATTERN = re.compile(r"^[+-]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?$")


def _coerce_number(value: str):
    text = value.strip()
    if text == "":
        return None
    if _INT_PATTERN.match(text):
        try:
            return int(text)
        except ValueError:
            return text
    if _FLOAT_PATTERN.match(text):
        try:
            return float(text)
        except ValueError:
            return text
    return text


def parse_payload_block(block: str) -> Iterable[Dict]:
    for raw_line in block.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 7:
            raise ValueError(
                f"Payload line has {len(parts)} columns (<7 expected): {raw_line!r}"
            )
        yield {
            "packet": _coerce_number(parts[1]),
            "type_tag": parts[3],  # Kept for grouping, excluded from CSV output
            # Keep payload entries as strings to avoid mixed numeric/string types.
            "payload": [v.strip() for v in parts[6:] if v.strip() != ""],
        }


def parse_file(path: Path) -> Iterable[Dict]:
    with path.open(newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row_number, row in enumerate(reader, start=2):  # account for header line
            payload_blob: Optional[str] = row.get("payload")
            if payload_blob is None:
                continue
            for payload_row in parse_payload_block(payload_blob):
                yield {
                    "timestamp_iso8601": row.get("timestamp_iso8601"),
                    "timestamp_epoch_ms": _coerce_number(
                        row.get("timestamp_epoch_ms", "")
                    ),
                    **payload_row,
                }


def serialize_payload(values: List) -> str:
    return json.dumps([str(v) for v in values], ensure_ascii=True, separators=(",", ":"))


FIELDNAMES = [
    "timestamp_iso8601",
    "timestamp_epoch_ms",
    "packet",
    "payload",
]


def write_jsonl(records: Iterable[Dict], stream: TextIO) -> None:
    for record in records:
        stream.write(json.dumps(record, ensure_ascii=True) + "\n")


def write_csv(records: Iterable[Dict], stream: TextIO) -> None:
    writer = csv.DictWriter(stream, fieldnames=FIELDNAMES, extrasaction="ignore")
    writer.writeheader()
    for record in records:
        row = dict(record)
        row["payload"] = serialize_payload(row.get("payload") or [])
        writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse EmotiBit payload lines from a CSV into CSV (default) or JSONL."
    )
    parser.add_argument(
        "input",
        nargs="?",
        default="sample.csv",
        type=Path,
        help="CSV file containing a 'payload' column (default: sample.csv)",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "jsonl"],
        default="csv",
        help="Output format (default: csv).",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Optional path to write output (defaults to stdout).",
    )
    args = parser.parse_args()

    records = parse_file(args.input)
    output_stream: TextIO
    needs_close = False

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        output_stream = args.output.open(
            "w", encoding="utf-8", newline="" if args.format == "csv" else None
        )
        needs_close = True
    else:
        # Best effort to avoid extra blank lines on some platforms when writing CSV.
        try:
            sys.stdout.reconfigure(newline="")  # type: ignore[attr-defined]
        except Exception:
            pass
        output_stream = sys.stdout

    try:
        if args.format == "jsonl":
            write_jsonl(records, output_stream)
        else:
            write_csv(records, output_stream)
    except BrokenPipeError:
        # Allow piping to tools like `head` without stack traces.
        sys.exit(0)
    finally:
        if needs_close:
            output_stream.close()

if __name__ == "__main__":
    main()
