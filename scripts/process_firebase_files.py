"""
Firebase Storage file processor for GitHub Actions.

Downloads unparsed CSV files from Firebase Storage, runs parse_payload.py,
and uploads the parsed results back to Firebase Storage.

Expected Firebase Storage structure:
  Input:  recordings/$uid/$uid-yyyymmdd.csv
          recordings/$uid/$uid-yyyymmdd-location.csv
  Output: parsed/$uid/$type_tag/$yyyymmdd.csv (type_tag includes "location")

Environment variables required:
  FIREBASE_CREDENTIALS: JSON string of Firebase service account credentials
  FIREBASE_BUCKET: Firebase Storage bucket name (e.g., "my-project.appspot.com")

Optional environment variables:
  OUTPUT_FORMAT: Output format - "csv" or "jsonl" (default: "csv")
"""

import csv
import json
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, storage


# Add parent directory to path so we can import parse_payload
sys.path.insert(0, str(Path(__file__).parent.parent))
import parse_payload

from datetime import datetime, timezone


def expand_payload_records(records: list) -> list:
    """
    Expand records with multiple payload values into separate rows.
    Uses interpolation to estimate timestamps for each value.

    For example, if a record has payload ["0.223", "0.224", "0.224"] and the next
    record is 24ms later, each value gets a timestamp 8ms apart.

    Args:
        records: List of parsed records (must be sorted by timestamp)

    Returns:
        List of expanded records, each with a single payload value
    """
    if not records:
        return []

    expanded = []

    for i, record in enumerate(records):
        payload = record.get("payload", [])
        if not payload:
            continue

        num_values = len(payload)
        current_ts_ms = record.get("timestamp_epoch_ms")
        current_ts_iso = record.get("timestamp_iso8601")
        packet = record.get("packet")

        if num_values == 1:
            # Single value, no expansion needed
            expanded.append({
                "timestamp_iso8601": current_ts_iso,
                "timestamp_epoch_ms": current_ts_ms,
                "packet": packet,
                "payload": payload[0],
            })
            continue

        # Calculate interval between this record and the next
        if i + 1 < len(records):
            next_ts_ms = records[i + 1].get("timestamp_epoch_ms")
            if current_ts_ms is not None and next_ts_ms is not None:
                total_interval = next_ts_ms - current_ts_ms
                interval_per_value = total_interval / num_values
            else:
                # Default to 8ms if timestamps are missing
                interval_per_value = 8.0
        else:
            # Last record - estimate based on typical sampling rate
            # Default to 8ms per value (125 Hz)
            interval_per_value = 8.0

        # Expand each payload value into its own row
        for j, value in enumerate(payload):
            if current_ts_ms is not None:
                interpolated_ts_ms = current_ts_ms + (j * interval_per_value)
                # Convert to ISO8601
                dt = datetime.fromtimestamp(interpolated_ts_ms / 1000.0, tz=timezone.utc)
                interpolated_ts_iso = dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(interpolated_ts_ms % 1000):03d}Z"
            else:
                interpolated_ts_ms = None
                interpolated_ts_iso = current_ts_iso

            expanded.append({
                "timestamp_iso8601": interpolated_ts_iso,
                "timestamp_epoch_ms": interpolated_ts_ms,
                "packet": packet,
                "payload": value,
            })

    return expanded


def init_firebase() -> storage.bucket:
    """Initialize Firebase and return the storage bucket."""
    creds_json = os.environ.get("FIREBASE_CREDENTIALS")
    if not creds_json:
        raise RuntimeError("FIREBASE_CREDENTIALS environment variable not set")

    bucket_name = os.environ.get("FIREBASE_BUCKET")
    if not bucket_name:
        raise RuntimeError("FIREBASE_BUCKET environment variable not set")

    cred = credentials.Certificate(json.loads(creds_json))
    firebase_admin.initialize_app(cred, {"storageBucket": bucket_name})

    return storage.bucket()


def get_unparsed_files(bucket, input_prefix: str, output_prefix: str) -> tuple:
    """
    List files in input_prefix that don't have a corresponding parsed file
    in output_prefix.

    Input structure:  recordings/$uid/$uid-yyyymmdd.csv
                      recordings/$uid/$uid-yyyymmdd-location.csv
    Output structure: parsed/$uid/$type_tag/$yyyymmdd.csv (location is a type_tag)

    Returns:
        tuple: (data_files, location_files) - lists of blobs to process
    """
    input_blobs = list(bucket.list_blobs(prefix=input_prefix))

    # Build set of already-parsed files
    # e.g., "parsed/abc123/HR/20241201.csv" means abc123-20241201 HR data is parsed
    # e.g., "parsed/abc123/location/20241201.csv" means abc123-20241201 location is parsed
    parsed_data = set()
    parsed_location = set()

    for blob in bucket.list_blobs(prefix=output_prefix):
        parts = blob.name[len(output_prefix):].split("/")
        if len(parts) >= 3:
            uid = parts[0]
            tag_folder = parts[1]
            filename = parts[2]
            # Filename is now just yyyymmdd.csv
            date_part = filename.split(".")[0]
            if tag_folder == "location":
                parsed_location.add(f"{uid}/{uid}-{date_part}-location")
            else:
                parsed_data.add(f"{uid}/{uid}-{date_part}")

    data_files = []
    location_files = []

    for blob in input_blobs:
        # Skip "directories" (empty blobs ending with /)
        if blob.name.endswith("/"):
            continue
        # Skip non-CSV files
        if not blob.name.lower().endswith(".csv"):
            continue

        # Get path relative to input prefix: $uid/$uid-yyyymmdd.csv or $uid/$uid-yyyymmdd-location.csv
        relative_path = blob.name[len(input_prefix):]
        # Remove .csv extension
        base_path = relative_path.rsplit(".", 1)[0]

        if base_path.endswith("-location"):
            # Location file
            if base_path not in parsed_location:
                location_files.append(blob)
        else:
            # Data file
            if base_path not in parsed_data:
                data_files.append(blob)

    return data_files, location_files


def move_location_file(
    blob, bucket, input_prefix: str, output_prefix: str, temp_dir: Path
) -> bool:
    """
    Move a location file without parsing.
    Returns True if successful, False otherwise.

    Output structure:
      recordings/$uid/$uid-yyyymmdd-location.csv -> parsed/$uid/location/$yyyymmdd.csv
    """
    temp_path = temp_dir / "location.csv"

    print(f"Moving location file: {blob.name}")

    # Download the file
    print(f"  Downloading...")
    blob.download_to_filename(str(temp_path))

    # Extract uid and date from blob name
    # e.g., recordings/abc123/abc123-20241201-location.csv -> uid=abc123, date=20241201
    relative_path = blob.name[len(input_prefix):]  # $uid/$uid-yyyymmdd-location.csv
    path_parts = relative_path.split("/")
    uid = path_parts[0]
    filename = path_parts[-1]  # $uid-yyyymmdd-location.csv
    # Remove -location.csv suffix, then get date
    base_name = filename.rsplit("-location", 1)[0]  # $uid-yyyymmdd
    date_part = base_name.rsplit("-", 1)[-1]  # yyyymmdd

    # Upload: parsed/$uid/location/$yyyymmdd.csv
    output_blob_name = f"{output_prefix}{uid}/location/{date_part}.csv"
    print(f"  Uploading to: {output_blob_name}")
    output_blob = bucket.blob(output_blob_name)
    output_blob.upload_from_filename(str(temp_path), content_type="text/csv")

    print(f"  Done!")
    return True


def process_file(
    blob, bucket, input_prefix: str, output_prefix: str, output_format: str, temp_dir: Path
) -> bool:
    """
    Download a file, parse it, and upload separate files per type_tag.
    Returns True if successful, False otherwise.

    Output structure:
      recordings/$uid/$uid-yyyymmdd.csv -> parsed/$uid/$type_tag/$yyyymmdd.csv
    """
    input_path = temp_dir / "input.csv"
    extension = "csv" if output_format == "csv" else "jsonl"

    print(f"Processing: {blob.name}")

    # Download the file
    print(f"  Downloading...")
    blob.download_to_filename(str(input_path))

    # Extract uid and date from blob name
    # e.g., recordings/abc123/abc123-20241201.csv -> uid=abc123, date=20241201
    relative_path = blob.name[len(input_prefix):]  # $uid/$uid-yyyymmdd.csv
    path_parts = relative_path.split("/")
    uid = path_parts[0]
    filename = path_parts[-1]  # $uid-yyyymmdd.csv
    date_part = filename.rsplit("-", 1)[-1].split(".")[0]  # yyyymmdd

    # Parse the file and group records by type_tag
    print(f"  Parsing and grouping by type_tag...")
    try:
        records_by_tag = defaultdict(list)
        for record in parse_payload.parse_file(input_path):
            type_tag = record.get("type_tag", "UNKNOWN")
            records_by_tag[type_tag].append(record)
    except Exception as e:
        print(f"  ERROR parsing: {e}")
        return False

    if not records_by_tag:
        print(f"  No records found")
        return True

    # Upload a separate file for each type_tag
    content_type = "text/csv" if output_format == "csv" else "application/x-ndjson"
    upload_count = 0

    # Fieldnames for expanded records (payload is now a single value, not a list)
    expanded_fieldnames = ["timestamp_iso8601", "timestamp_epoch_ms", "packet", "payload"]

    for type_tag, records in records_by_tag.items():
        output_path = temp_dir / f"output_{type_tag}.{extension}"

        # Expand payload records (split multi-value payloads into separate rows)
        expanded_records = expand_payload_records(records)

        # Write records for this type_tag
        with output_path.open("w", encoding="utf-8", newline="" if output_format == "csv" else None) as f:
            if output_format == "jsonl":
                for record in expanded_records:
                    f.write(json.dumps(record, ensure_ascii=True) + "\n")
            else:
                writer = csv.DictWriter(f, fieldnames=expanded_fieldnames)
                writer.writeheader()
                writer.writerows(expanded_records)

        # Upload: parsed/$uid/$type_tag/$yyyymmdd.csv
        output_blob_name = f"{output_prefix}{uid}/{type_tag}/{date_part}.{extension}"
        print(f"  Uploading: {output_blob_name} ({len(expanded_records)} rows from {len(records)} records)")
        output_blob = bucket.blob(output_blob_name)
        output_blob.upload_from_filename(str(output_path), content_type=content_type)
        upload_count += 1

    print(f"  Done! Uploaded {upload_count} files for {len(records_by_tag)} type tags")
    return True


def main() -> int:
    input_prefix = "recordings/"
    output_prefix = "parsed/"
    output_format = os.environ.get("OUTPUT_FORMAT", "csv")

    if output_format not in ("csv", "jsonl"):
        print(f"Invalid OUTPUT_FORMAT: {output_format}. Must be 'csv' or 'jsonl'.")
        return 1

    print("Initializing Firebase...")
    bucket = init_firebase()

    print(f"Looking for unparsed files in '{input_prefix}'...")
    data_files, location_files = get_unparsed_files(bucket, input_prefix, output_prefix)

    if not data_files and not location_files:
        print("No unparsed files found.")
        return 0

    print(f"Found {len(data_files)} data file(s) and {len(location_files)} location file(s).")

    success_count = 0
    error_count = 0

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Process data files (parse and split by type_tag)
        for blob in data_files:
            if process_file(blob, bucket, input_prefix, output_prefix, output_format, temp_path):
                success_count += 1
            else:
                error_count += 1

        # Move location files (no parsing)
        for blob in location_files:
            if move_location_file(blob, bucket, input_prefix, output_prefix, temp_path):
                success_count += 1
            else:
                error_count += 1

    print(f"\nSummary: {success_count} succeeded, {error_count} failed")
    return 1 if error_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
