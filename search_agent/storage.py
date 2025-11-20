import glob
import os
from io import BytesIO
from abc import ABC, abstractmethod
from datetime import date
from typing import List, Optional

import pandas as pd


def default_date_str(d: Optional[date] = None) -> str:
    return (d or date.today()).strftime("%Y%m%d")


class StorageClient(ABC):
    @abstractmethod
    def list_parquet_with_date(self, prefix: str, date_str: str) -> List[str]:
        ...

    @abstractmethod
    def read_parquet(self, path: str) -> pd.DataFrame:
        ...

    @abstractmethod
    def write_parquet(self, df: pd.DataFrame, path: str) -> None:
        ...


class LocalStorageClient(StorageClient):
    """
    本地文件系统版，便于开发调试。
    支持前缀 + 日期的 glob 查询，例如 prefix=/data/incr/query_，日期=20240101，会匹配 query_20240101*.parquet。
    """

    def list_parquet_with_date(self, prefix: str, date_str: str) -> List[str]:
        import os

        # 方式0：直接指定单个parquet文件
        if prefix.endswith('.parquet'):
            if os.path.isfile(prefix):
                return [prefix]
            else:
                return []

        # 方式1：文件名前缀模式，例如 prefix=/data/query_, date=20251119 -> /data/query_20251119*.parquet
        pattern = f"{prefix}{date_str}*.parquet"
        files = sorted(glob.glob(pattern))

        # 方式2：目录模式，例如 prefix=/data/query_20251119/ -> /data/query_20251119/*.parquet
        if not files:
            # 尝试将prefix作为目录路径，查找其中所有parquet文件
            dir_pattern = f"{prefix.rstrip('/')}/*.parquet"
            files = sorted(glob.glob(dir_pattern))

        # 方式3：prefix+date作为目录，例如 prefix=/data/query_, date=20251119 -> /data/query_20251119/*.parquet
        if not files:
            dir_pattern = f"{prefix}{date_str}/*.parquet"
            files = sorted(glob.glob(dir_pattern))

        return files

    def read_parquet(self, path: str) -> pd.DataFrame:
        return pd.read_parquet(path)

    def write_parquet(self, df: pd.DataFrame, path: str) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        df.to_parquet(path, index=False)


class OssStorageClient(StorageClient):
    """
    OSS 存储客户端，使用时需要安装并配置 oss2。
    环境变量示例：
      OSS_ENDPOINT, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_BUCKET
    prefix 示例: oss://bucket/path/to/prefix_
    """

    def __init__(self, endpoint: str, access_key_id: str, access_key_secret: str, bucket_name: str) -> None:
        try:  # 延迟导入，避免本地无依赖时报错
            import oss2
        except ImportError as exc:  # noqa: PERF203
            raise RuntimeError("oss2 is required for OSS operations, please pip install oss2") from exc

        self.oss2 = oss2
        auth = oss2.Auth(access_key_id, access_key_secret)
        self.bucket = oss2.Bucket(auth, endpoint, bucket_name)

    def _split_bucket_key(self, path: str) -> str:
        # path 形如 oss://bucket/key
        _, _, rest = path.split("/", 2)
        bucket_name, key = rest.split("/", 1)
        if bucket_name != self.bucket.bucket_name:
            raise ValueError(f"path bucket {bucket_name} not equal to client bucket {self.bucket.bucket_name}")
        return key

    def list_parquet_with_date(self, prefix: str, date_str: str) -> List[str]:
        # prefix 形如 oss://bucket/path/prefix_
        key_prefix = self._split_bucket_key(prefix)
        key_prefix = f"{key_prefix}{date_str}"
        return [
            f"oss://{self.bucket.bucket_name}/{obj.key}"
            for obj in self.bucket.list_objects(prefix=key_prefix).object_list
            if obj.key.endswith(".parquet")
        ]

    def read_parquet(self, path: str) -> pd.DataFrame:
        key = self._split_bucket_key(path)
        result = self.bucket.get_object(key)
        # 需要将流读成 bytes 再交给 pandas
        return pd.read_parquet(BytesIO(result.read()))

    def write_parquet(self, df: pd.DataFrame, path: str) -> None:
        key = self._split_bucket_key(path)
        buf = BytesIO()
        df.to_parquet(buf, index=False)
        self.bucket.put_object(key, buf.getvalue())


def build_output_path(prefix: str, date_str: str, filename: str) -> str:
    return f"{prefix.rstrip('/')}/{date_str}/{filename}"


def choose_storage_client(prefers_oss: bool = False, oss_kwargs: Optional[dict] = None) -> StorageClient:
    if prefers_oss:
        if not oss_kwargs:
            raise RuntimeError("OSS client requested but configuration missing")
        return OssStorageClient(**oss_kwargs)
    return LocalStorageClient()
