import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.config import (
    HTTP_TIMEOUT,
    SOURCE_PARQUET_PATH,
    TABLE_NAME,
    TARGET_BUCKET,
    TARGET_PREFIX,
    MAX_FUTURES_BUFFER,
    MAX_WORKERS,
)

from dotenv import load_dotenv
from botocore.config import Config
from botocore.exceptions import ClientError
import boto3
import requests
import pyarrow.dataset as ds
from tenacity import retry, stop_after_attempt, wait_exponential

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


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def download_and_upload_image(url: str, s3_key: str):
    if s3_object_exists(TARGET_BUCKET, s3_key):
        log.info("SKIP exists %s", s3_key)
        return

    response = session.get(url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()

    s3.put_object(
        Bucket=TARGET_BUCKET,
        Key=s3_key,
        Body=response.content,
        ContentType="image/jpeg",
    )

    log.info("UPLOADED %s", s3_key)


def s3_object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def process_row(adid: str, pictureurls: list[str]):
    if not isinstance(pictureurls, list):
        return

    for idx, url in enumerate(pictureurls):
        if not url:
            continue

        s3_key = f"{TARGET_PREFIX}/{TABLE_NAME}/{adid}/{idx}.jpeg"

        try:
            download_and_upload_image(url, s3_key)
        except Exception as e:
            log.error(
                "FAILED adid=%s url=%s error=%s",
                adid,
                url,
                e,
            )


def main():
    log.info("Starting image transfer job")

    dataset = ds.dataset(
        SOURCE_PARQUET_PATH,
        format="parquet",
    )

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = []

    for batch in dataset.to_batches(columns=["adid", "pictureurls"]):
        rows = batch.to_pylist()

        for row in rows:
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
    log.info("DONE")


if __name__ == "__main__":
    main()
