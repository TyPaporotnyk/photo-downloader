SOURCE_PARQUET_PATH = "s3://yanport-images-historical/athena-unload/"
TARGET_BUCKET = "yanport-images-historical"
TARGET_PREFIX = "images"
TABLE_NAME = "raw_yanport"

MAX_WORKERS = 32
HTTP_TIMEOUT = 10
MAX_FUTURES_BUFFER = 10_000
