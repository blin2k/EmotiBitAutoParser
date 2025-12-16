"""
Firebase Storage file processor for GitHub Actions.

Downloads unparsed CSV files from Firebase Storage, runs parse_payload.py,
and uploads the parsed results back to Firebase Storage.

Expected Firebase Storage structure:
  Input:  recordings/$uid/$uid-yyyymmdd.csv
  Output: parsed/$uid/$type_tag/$uid_$yyyymmdd_$type_tag.csv

Environment variables required:
  FIREBASE_CREDENTIALS: JSON string of Firebase service account credentials
  FIREBASE_BUCKET: Firebase Storage bucket name (e.g., "my-project.appspot.com")

Optional environment variables:
  OUTPUT_FORMAT: Output format - "csv" or "jsonl" (default: "csv")
"""

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


def get_unparsed_files(bucket, input_prefix: str, output_prefix: str) -> list:
    """
    List files in input_prefix that don't have a corresponding parsed file
    in output_prefix.

    Input structure:  recordings/$uid/$uid-yyyymmdd.csv
    Output structure: parsed/$uid/$type_tag/$uid_$yyyymmdd_$type_tag.csv
    """
    input_blobs = list(bucket.list_blobs(prefix=input_prefix))

    # Build set of already-parsed files by checking for marker files
    # We check if any type_tag subfolder exists for a given uid/date combination
    # e.g., "parsed/abc123/HR/abc123_20241201_HR.csv" means abc123-20241201 is parsed
    output_blobs = set()
    for blob in bucket.list_blobs(prefix=output_prefix):
        # Extract uid and date from path like: parsed/$uid/$type_tag/$uid_$yyyymmdd_$type_tag.csv
        parts = blob.name[len(output_prefix):].split("/")
        if len(parts) >= 3:
            uid = parts[0]
            filename = parts[2]
            # Extract date from filename like: uid_yyyymmdd_typetag.csv
            # Split by _ to get [uid, yyyymmdd, typetag.csv]
            filename_parts = filename.split("_")
            if len(filename_parts) >= 2:
                date_part = filename_parts[1]  # Get yyyymmdd
                output_blobs.add(f"{uid}/{uid}-{date_part}")

    unparsed = []
    for blob in input_blobs:
        # Skip "directories" (empty blobs ending with /)
        if blob.name.endswith("/"):
            continue
        # Skip non-CSV files
        if not blob.name.lower().endswith(".csv"):
            continue

        # Get path relative to input prefix: $uid/$uid-yyyymmdd.csv
        relative_path = blob.name[len(input_prefix):]
        # Remove .csv extension: $uid/$uid-yyyymmdd
        base_path = relative_path.rsplit(".", 1)[0]

        if base_path not in output_blobs:
            unparsed.append(blob)

    return unparsed


def process_file(
    blob, bucket, input_prefix: str, output_prefix: str, output_format: str, temp_dir: Path
) -> bool:
    """
    Download a file, parse it, and upload separate files per type_tag.
    Returns True if successful, False otherwise.

    Output structure:
      recordings/$uid/$uid-yyyymmdd.csv -> parsed/$uid/$type_tag/$uid_$yyyymmdd_$type_tag.csv
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

    for type_tag, records in records_by_tag.items():
        output_path = temp_dir / f"output_{type_tag}.{extension}"

        # Write records for this type_tag
        with output_path.open("w", encoding="utf-8", newline="" if output_format == "csv" else None) as f:
            if output_format == "jsonl":
                parse_payload.write_jsonl(iter(records), f)
            else:
                parse_payload.write_csv(iter(records), f)

        # Upload: parsed/$uid/$type_tag/$uid_$yyyymmdd_$type_tag.csv
        output_blob_name = f"{output_prefix}{uid}/{type_tag}/{uid}_{date_part}_{type_tag}.{extension}"
        print(f"  Uploading: {output_blob_name} ({len(records)} records)")
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
    unparsed_files = get_unparsed_files(bucket, input_prefix, output_prefix)

    if not unparsed_files:
        print("No unparsed files found.")
        return 0

    print(f"Found {len(unparsed_files)} unparsed file(s).")

    success_count = 0
    error_count = 0

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        for blob in unparsed_files:
            if process_file(blob, bucket, input_prefix, output_prefix, output_format, temp_path):
                success_count += 1
            else:
                error_count += 1

    print(f"\nSummary: {success_count} succeeded, {error_count} failed")
    return 1 if error_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
