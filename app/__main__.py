import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
import boto3
import requests
from requests.adapters import HTTPAdapter
import pyarrow.dataset as ds
from botocore.config import Config
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import (
    CPU_COUNT,
    HTTP_TIMEOUT,
    SOURCE_PARQUET_PATH,
    TABLE_NAME,
    TARGET_BUCKET,
    TARGET_PREFIX,
    MAX_FUTURES_BUFFER,
    MAX_WORKERS,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

s3 = boto3.client(
    "s3",
    config=Config(
        max_pool_connections=MAX_WORKERS * 2,
        retries={"max_attempts": 10, "mode": "standard"},
    ),
)

session = requests.Session()
adapter = HTTPAdapter(
    pool_connections=MAX_WORKERS * 2,
    pool_maxsize=MAX_WORKERS * 2,
)
session.mount("http://", adapter)
session.mount("https://", adapter)
downloaded_count = 0
skipped_count = 0
failed_count = 0

counter_lock = threading.Lock()
stop_event = threading.Event()


def stats_logger(interval_seconds: int = 60):
    while True:
        if stop_event.wait(interval_seconds):
            break
        with counter_lock:
            d = downloaded_count
            s = skipped_count
            f = failed_count
        log.info("[STATS] downloaded=%d skipped=%d failed=%d", d, s, f)


def s3_object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def download_and_upload_image(url: str, s3_key: str):
    global skipped_count
    if s3_object_exists(TARGET_BUCKET, s3_key):
        with counter_lock:
            skipped_count += 1
        return
    _download_and_upload_image(url, s3_key)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def _download_and_upload_image(url: str, s3_key: str):
    global downloaded_count
    response = session.get(url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    s3.put_object(
        Bucket=TARGET_BUCKET,
        Key=s3_key,
        Body=response.content,
        ContentType="image/jpeg",
    )
    with counter_lock:
        downloaded_count += 1


def process_row(adid: str, pictureurls: list[str]):
    global failed_count
    if not isinstance(pictureurls, list):
        return
    for idx, url in enumerate(pictureurls):
        if not url:
            continue
        s3_key = f"{TARGET_PREFIX}/{TABLE_NAME}/{adid}/{idx}.jpeg"
        try:
            download_and_upload_image(url, s3_key)
        except Exception:
            with counter_lock:
                failed_count += 1


def main():
    log.info("Starting image transfer job")
    log.info("Using %d workers (cpu=%d)", MAX_WORKERS, CPU_COUNT)

    stats_thread = threading.Thread(target=stats_logger, daemon=True)
    stats_thread.start()

    dataset = ds.dataset(SOURCE_PARQUET_PATH, format="parquet")

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = []

    for batch in dataset.to_batches(columns=["adid", "pictureurls"]):
        for row in batch.to_pylist():
            futures.append(
                executor.submit(
                    process_row,
                    row["adid"],
                    row["pictureurls"],
                )
            )
            if len(futures) >= MAX_FUTURES_BUFFER:
                for f in as_completed(futures):
                    pass
                futures.clear()

    for f in as_completed(futures):
        pass

    executor.shutdown(wait=True)

    stop_event.set()
    stats_thread.join(timeout=1)

    log.info("=== FINAL RESULT ===")
    log.info("Downloaded images: %d", downloaded_count)
    log.info("Skipped (already exists): %d", skipped_count)
    log.info("Failed: %d", failed_count)
    log.info("DONE")


if __name__ == "__main__":
    main()
