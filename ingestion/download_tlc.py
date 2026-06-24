"""Downloads 2023 NYC TLC Yellow Taxi trip data and uploads it to ADLS raw zone."""

import logging
import os
import sys
import tempfile

import requests
from azure.storage.blob import BlobServiceClient, ContentSettings

SOURCE_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{month}.parquet"
)
CONTAINER_NAME = "raw"
BLOB_PREFIX = "tlc"
DOWNLOAD_CHUNK_SIZE = 1024 ** 2  # 1MiB
PROGRESS_LOG_INTERVAL = 10 * 1024 ** 2  # log every 10MiB transferred
UPLOAD_BLOCK_SIZE = 4 * 1024 ** 2  # force chunked Put Block uploads in 4MiB pieces
MAX_DOWNLOAD_ATTEMPTS = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("azure").setLevel(logging.WARNING)
logger = logging.getLogger("download_tlc")


def human_size(num_bytes):
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}PB"


def get_blob_service_client():
    connection_string = os.environ.get("ADLS_CONNECTION_STRING")
    if not connection_string:
        raise RuntimeError("ADLS_CONNECTION_STRING environment variable is not set")
    return BlobServiceClient.from_connection_string(
        connection_string,
        # this environment's outbound bandwidth is slow enough that a single 45MB+ PUT
        # regularly times out mid-transfer; smaller blocks keep each request short enough
        # to complete, and let the SDK retry just the failed block instead of the whole file
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

    fd, tmp_path = tempfile.mkstemp(suffix=".parquet", prefix="tlc_")
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

        # resume from where the last attempt was cut off, instead of restarting the
        # whole file, since the connection here drops mid-transfer often enough to matter
        headers = {"Range": f"bytes={written}-"} if written else {}
        try:
            with requests.get(url, headers=headers, stream=True, timeout=(30, 60)) as response:
                response.raise_for_status()
                with open(tmp_path, "r+b" if written else "wb") as tmp_file:
                    tmp_file.seek(written)
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                        tmp_file.write(chunk)
                        written += len(chunk)
                        progress(written)
        except requests.exceptions.RequestException as exc:
            logger.info(
                "  ...%s download interrupted at %s, retrying (attempt %d/%d): %s",
                label, human_size(written), attempt, MAX_DOWNLOAD_ATTEMPTS, exc,
            )

    return tmp_path


def process_month(blob_service_client, month):
    url = SOURCE_URL_TEMPLATE.format(month=month)
    file_name = f"yellow_tripdata_2023-{month}.parquet"
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

    try:
        logger.info("Uploading %s -> %s/%s", file_name, CONTAINER_NAME, blob_name)
        # downloading to a local file first (instead of piping the HTTP response straight
        # into upload_blob) makes the source seekable, so the SDK can safely retry a
        # transient network failure without us having to re-fetch already-consumed bytes
        with open(tmp_path, "rb") as tmp_file:
            blob_client.upload_blob(
                data=tmp_file,
                length=expected_size,
                overwrite=True,
                content_settings=ContentSettings(content_type="application/octet-stream"),
                progress_hook=make_progress_logger(file_name, "upload", expected_size),
            )
    finally:
        os.remove(tmp_path)

    logger.info("Done: %s (%s)", file_name, human_size(expected_size))
    return True


def main():
    blob_service_client = get_blob_service_client()

    succeeded = 0
    for i in range(1, 13):
        month = f"{i:02d}"
        try:
            if process_month(blob_service_client, month):
                succeeded += 1
        except Exception as exc:
            logger.error("Failed processing month %s: %s", month, exc)

    print(f"Summary: {succeeded} of 12 files succeeded")
    if succeeded < 12:
        sys.exit(1)


if __name__ == "__main__":
    main()
