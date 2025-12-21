"""
Download all files from the parsed/ folder in Firebase Storage.

Usage:
  python scripts/download_parsed.py
  python scripts/download_parsed.py --output ./downloads
  python scripts/download_parsed.py --uid abc123

Loads credentials from .env file in the project root.
"""

import argparse
import json
import os
import sys
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, storage

# Project root directory (parent of scripts/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_env():
    """Load environment variables from .env file."""
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        print(f"Warning: .env file not found at {env_path}")
        return

    with env_path.open() as f:
        content = f.read()

    # Parse .env file - handle multi-line JSON value
    lines = content.split("\n")
    current_key = None
    current_value = []
    brace_count = 0

    for line in lines:
        if current_key:
            # We're in a multi-line value
            current_value.append(line)
            brace_count += line.count("{") - line.count("}")
            if brace_count == 0:
                # JSON object is complete
                os.environ[current_key] = "\n".join(current_value)
                current_key = None
                current_value = []
        elif "=" in line:
            # Check if line starts with a valid key (letters, numbers, underscore)
            eq_pos = line.find("=")
            key = line[:eq_pos].strip()
            value = line[eq_pos + 1:].strip()

            if not key or not key.replace("_", "").isalnum():
                continue

            # Check if this is the start of a multi-line JSON value
            brace_count = value.count("{") - value.count("}")
            if brace_count > 0:
                current_key = key
                current_value = [value]
            else:
                os.environ[key] = value


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


def download_parsed_files(output_dir: Path, uid_filter: str = None) -> int:
    """
    Download all files from parsed/ folder.

    Args:
        output_dir: Local directory to save files
        uid_filter: Optional UID to filter downloads

    Returns:
        Number of files downloaded
    """
    print("Initializing Firebase...")
    bucket = init_firebase()

    prefix = "parsed/"
    if uid_filter:
        prefix = f"parsed/{uid_filter}/"

    print(f"Listing files in '{prefix}'...")
    blobs = list(bucket.list_blobs(prefix=prefix))

    # Filter out "directories" (empty blobs ending with /)
    files = [b for b in blobs if not b.name.endswith("/")]

    if not files:
        print("No files found.")
        return 0

    print(f"Found {len(files)} file(s) to download.")

    download_count = 0
    for blob in files:
        # Create local path: output_dir/parsed/$uid/$type_tag/$yyyymmdd.csv
        relative_path = blob.name  # e.g., parsed/abc123/HR/20241201.csv
        local_path = output_dir / relative_path

        # Create parent directories
        local_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"Downloading: {blob.name}")
        blob.download_to_filename(str(local_path))
        download_count += 1

    print(f"\nDownloaded {download_count} file(s) to {output_dir}")
    return download_count


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download all parsed files from Firebase Storage."
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=Path("./downloads"),
        help="Output directory (default: ./downloads)",
    )
    parser.add_argument(
        "--uid",
        type=str,
        default=None,
        help="Filter by specific UID",
    )
    args = parser.parse_args()

    # Load .env file
    load_env()

    try:
        download_parsed_files(args.output, args.uid)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
