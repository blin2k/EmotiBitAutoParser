"""
Firebase Storage file processor for GitHub Actions.

Downloads unparsed CSV files from Firebase Storage, runs parse_payload.py,
and uploads the parsed results back to Firebase Storage.

Environment variables required:
  FIREBASE_CREDENTIALS: JSON string of Firebase service account credentials
  FIREBASE_BUCKET: Firebase Storage bucket name (e.g., "my-project.appspot.com")

Optional environment variables:
  INPUT_PREFIX: Prefix/folder for unparsed files (default: "raw/")
  OUTPUT_PREFIX: Prefix/folder for parsed files (default: "parsed/")
  OUTPUT_FORMAT: Output format - "csv" or "jsonl" (default: "csv")
"""

import json
import os
import sys
import tempfile
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
    """
    input_blobs = list(bucket.list_blobs(prefix=input_prefix))
    output_blobs = set(
        blob.name.replace(output_prefix, "").replace(".parsed", "")
        for blob in bucket.list_blobs(prefix=output_prefix)
    )

    unparsed = []
    for blob in input_blobs:
        # Skip "directories" (empty blobs ending with /)
        if blob.name.endswith("/"):
            continue
        # Skip non-CSV files
        if not blob.name.lower().endswith(".csv"):
            continue

        relative_name = blob.name.replace(input_prefix, "")
        # Check if parsed version exists (with .parsed suffix before extension)
        base_name = relative_name.rsplit(".", 1)[0]
        if base_name not in output_blobs:
            unparsed.append(blob)

    return unparsed


def process_file(
    blob, bucket, output_prefix: str, output_format: str, temp_dir: Path
) -> bool:
    """
    Download a file, parse it, and upload the result.
    Returns True if successful, False otherwise.
    """
    input_path = temp_dir / "input.csv"
    extension = "csv" if output_format == "csv" else "jsonl"
    output_path = temp_dir / f"output.{extension}"

    print(f"Processing: {blob.name}")

    # Download the file
    print(f"  Downloading...")
    blob.download_to_filename(str(input_path))

    # Parse the file
    print(f"  Parsing...")
    try:
        records = parse_payload.parse_file(input_path)
        with output_path.open("w", encoding="utf-8", newline="" if output_format == "csv" else None) as f:
            if output_format == "jsonl":
                parse_payload.write_jsonl(records, f)
            else:
                parse_payload.write_csv(records, f)
    except Exception as e:
        print(f"  ERROR parsing: {e}")
        return False

    # Determine output blob name
    relative_name = blob.name.split("/", 1)[-1] if "/" in blob.name else blob.name
    base_name = relative_name.rsplit(".", 1)[0]
    output_blob_name = f"{output_prefix}{base_name}.parsed.{extension}"

    # Upload the parsed file
    print(f"  Uploading to: {output_blob_name}")
    output_blob = bucket.blob(output_blob_name)
    output_blob.upload_from_filename(str(output_path))

    print(f"  Done!")
    return True


def main() -> int:
    input_prefix = os.environ.get("INPUT_PREFIX", "raw/")
    output_prefix = os.environ.get("OUTPUT_PREFIX", "parsed/")
    output_format = os.environ.get("OUTPUT_FORMAT", "csv")

    if output_format not in ("csv", "jsonl"):
        print(f"Invalid OUTPUT_FORMAT: {output_format}. Must be 'csv' or 'jsonl'.")
        return 1

    # Ensure prefixes end with /
    if not input_prefix.endswith("/"):
        input_prefix += "/"
    if not output_prefix.endswith("/"):
        output_prefix += "/"

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
            if process_file(blob, bucket, output_prefix, output_format, temp_path):
                success_count += 1
            else:
                error_count += 1

    print(f"\nSummary: {success_count} succeeded, {error_count} failed")
    return 1 if error_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
