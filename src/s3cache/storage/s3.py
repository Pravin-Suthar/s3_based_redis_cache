from __future__ import annotations

import logging
from typing import Any

from s3cache.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class S3Backend(StorageBackend):
    def __init__(
        self,
        bucket: str,
        prefix: str = "query-cache",
        region: str = "us-east-1",
        client: Any = None,
    ) -> None:
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        if client is not None:
            self._client = client
        else:
            import boto3

            self._client = boto3.client("s3", region_name=region)

    def _s3_key(self, key: str) -> str:
        return f"{self._prefix}/{key}"

    def get(self, key: str) -> bytes | None:
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=self._s3_key(key))
            return resp["Body"].read()  # type: ignore[no-any-return]
        except self._client.exceptions.NoSuchKey:
            return None
        except Exception:
            logger.warning("S3 get failed for key %s", key, exc_info=True)
            return None

    def put(self, key: str, data: bytes) -> None:
        self._client.put_object(
            Bucket=self._bucket,
            Key=self._s3_key(key),
            Body=data,
            ServerSideEncryption="AES256",
        )

    def delete(self, key: str) -> None:
        try:
            self._client.delete_object(Bucket=self._bucket, Key=self._s3_key(key))
        except Exception:
            logger.warning("S3 delete failed for key %s", key, exc_info=True)

    def list_keys(self, prefix: str = "") -> list[str]:
        keys: list[str] = []
        full_prefix = f"{self._prefix}/{prefix}"
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                k: str = obj["Key"]
                # Strip the S3 prefix to return just the cache key
                keys.append(k[len(self._prefix) + 1 :])
        return keys
