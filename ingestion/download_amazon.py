"""Downloads Amazon Reviews 2023 category files and uploads them to ADLS raw/amazon/."""

import logging
import os
import sys
import tempfile

import time

import requests
from azure.storage.blob import BlobServiceClient, ContentSettings

BASE_URL = (
    "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023"
    "/resolve/main/raw/review_categories/{filename}"
)
CATEGORIES = [
    "Automotive",
    "Cell_Phones_and_Accessories",
    "Clothing_Shoes_and_Jewelry",
    "Electronics",
    "Sports_and_Outdoors",
]
TMP_DIR        = os.path.expanduser("~/omnicart-intelligence-platform/tmp")
CONTAINER_NAME = "raw"
BLOB_PREFIX = "amazon"
DOWNLOAD_CHUNK_SIZE = 1024 ** 2          # 1 MiB
PROGRESS_LOG_INTERVAL = 50 * 1024 ** 2  # log every 50 MiB
UPLOAD_BLOCK_SIZE = 4 * 1024 ** 2       # 4 MiB chunks
MAX_DOWNLOAD_ATTEMPTS = 15
MAX_UPLOAD_ATTEMPTS = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("azure").setLevel(logging.WARNING)
logger = logging.getLogger("download_amazon")


def human_size(num_bytes):
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}PB"


os.makedirs(TMP_DIR, exist_ok=True)


def get_blob_service_client():
    connection_string = os.environ.get("ADLS_CONNECTION_STRING")
    if not connection_string:
        raise RuntimeError("ADLS_CONNECTION_STRING environment variable is not set")
    return BlobServiceClient.from_connection_string(
        connection_string,
        max_single_put_size=UPLOAD_BLOCK_SIZE,
        max_block_size=UPLOAD_BLOCK_SIZE,
    )


def get_remote_size(url):
    response = requests.head(url, allow_redirects=True, timeout=30)
    response.raise_for_status()
    return int(response.headers["Content-Length"])


def make_progress_logger(label, stage, total):
    next_log = [PROGRESS_LOG_INTERVAL]

    def report(current, _total=None):
        if current >= next_log[0] or current == total:
            pct = f" ({100 * current / total:.0f}%)" if total else ""
            logger.info("  ...%s %s: %s transferred%s", label, stage, human_size(current), pct)
            next_log[0] += PROGRESS_LOG_INTERVAL

    return report


def download_to_tempfile(url, expected_size, label):
    progress = make_progress_logger(label, "download", expected_size)

    fd, tmp_path = tempfile.mkstemp(suffix=".jsonl", prefix="amazon_", dir=TMP_DIR)
    os.close(fd)

    written = 0
    attempt = 0
    while written < expected_size:
        attempt += 1
        if attempt > MAX_DOWNLOAD_ATTEMPTS:
            raise RuntimeError(
                f"download interrupted after {attempt - 1} attempts at "
                f"{human_size(written)}/{human_size(expected_size)}"
            )

        headers = {"Range": f"bytes={written}-"} if written else {}
        try:
            with requests.get(url, headers=headers, stream=True, timeout=(30, 120)) as response:
                response.raise_for_status()
                with open(tmp_path, "r+b" if written else "wb") as tmp_file:
                    tmp_file.seek(written)
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                        tmp_file.write(chunk)
                        written += len(chunk)
                        progress(written)
        except requests.exceptions.RequestException as exc:
            backoff = min(2 ** attempt, 60)
            logger.info(
                "  ...%s download interrupted at %s, retrying in %ds (attempt %d/%d): %s",
                label, human_size(written), backoff, attempt, MAX_DOWNLOAD_ATTEMPTS, exc,
            )
            time.sleep(backoff)

    return tmp_path


def upload_with_retry(blob_client, tmp_path, expected_size, label):
    for attempt in range(1, MAX_UPLOAD_ATTEMPTS + 1):
        try:
            with open(tmp_path, "rb") as tmp_file:
                blob_client.upload_blob(
                    data=tmp_file,
                    length=expected_size,
                    overwrite=True,
                    content_settings=ContentSettings(content_type="application/x-ndjson"),
                    progress_hook=make_progress_logger(label, "upload", expected_size),
                )
            return
        except Exception as exc:
            if attempt >= MAX_UPLOAD_ATTEMPTS:
                raise
            backoff = min(2 ** attempt, 60)
            logger.info(
                "  ...%s upload failed, retrying in %ds (attempt %d/%d): %s",
                label, backoff, attempt, MAX_UPLOAD_ATTEMPTS, exc,
            )
            time.sleep(backoff)


def process_category(blob_service_client, category):
    file_name = f"{category}.jsonl"
    url = BASE_URL.format(filename=file_name)
    blob_name = f"{BLOB_PREFIX}/{file_name}"
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

    expected_size = get_remote_size(url)

    if blob_client.exists():
        existing_size = blob_client.get_blob_properties().size
        if existing_size == expected_size:
            logger.info("Skipping %s, already uploaded (%s)", file_name, human_size(existing_size))
            return True
        logger.info(
            "Re-uploading %s, existing blob size %s does not match source %s",
            file_name, human_size(existing_size), human_size(expected_size),
        )

    logger.info("Downloading %s (%s)", file_name, human_size(expected_size))
    tmp_path = download_to_tempfile(url, expected_size, file_name)

    logger.info("Uploading %s -> %s/%s", file_name, CONTAINER_NAME, blob_name)
    upload_with_retry(blob_client, tmp_path, expected_size, file_name)
    os.remove(tmp_path)

    logger.info("Done: %s (%s)", file_name, human_size(expected_size))
    return True


def main():
    blob_service_client = get_blob_service_client()

    total = len(CATEGORIES)
    succeeded = 0
    for category in CATEGORIES:
        try:
            if process_category(blob_service_client, category):
                succeeded += 1
        except Exception as exc:
            logger.error("Failed processing %s: %s", category, exc)

    print(f"Summary: {succeeded} of {total} succeeded")
    if succeeded < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
