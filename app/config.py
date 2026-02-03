import os

SOURCE_PARQUET_PATH = "s3://yanport-images-historical/athena-unload/"
TARGET_BUCKET = "yanport-images-historical"
TARGET_PREFIX = "images"
TABLE_NAME = "raw_yanport"

CPU_COUNT = os.cpu_count() or 1
MAX_WORKERS = min(CPU_COUNT * 4, 64)
HTTP_TIMEOUT = 10
MAX_FUTURES_BUFFER = 10_000
