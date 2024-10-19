from __future__ import annotations

import gzip
import pickle
import tempfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Dict, Optional
import json

import boto3
import pandas as pd
from botocore.exceptions import ClientError

OPTIVER_BUCKET_NAME = 'hkexsampledata'


@dataclass(frozen=True)
class S3BucketActions:
    resource: boto3.resource
    client: boto3.client
    bucket_name: str

    def _bucket(self):
        return self.resource.Bucket(self.bucket_name)

    def exists(self, path: str):
        return exists(self._bucket().Object(path))

    def delete(self, path: str):
        return self._bucket().Object(path).delete()

    def ls(self, path: str):
        return [item for item in self._bucket().objects.filter(Prefix=path)]

    def lss(self, path: str, recursive: bool = False) -> Iterable[str]:
        """
        ls(super)

        .ls(...) is incredibly slow on large "directories" as it pulls every entry.
        This improves on it by defaulting to not recurse through "subdirectories",
        and also returning an iterable allowing for itertools.islice to early exit.
        """
        paginator = self.client.get_paginator("list_objects_v2")  # type: ignore

        if recursive:
            yield from (
                content["Key"]
                for result in paginator.paginate(Bucket=self.bucket_name, Prefix=path)
                for content in result.get("Contents", [])
            )
        else:
            yield from (
                prefix_or_content["Prefix" if "Prefix" in prefix_or_content else "Key"]
                for result in paginator.paginate(
                    Bucket=self.bucket_name, Delimiter="/", Prefix=path
                )
                for prefix_or_content in result.get("CommonPrefixes", [])
                + result.get("Contents", [])
            )

    def upload_file(self, filename: str, location: str):
        return self._bucket().upload_file(filename, location)

    def upload_fileobj(self, file_obj, location: str):
        return self._bucket().upload_fileobj(file_obj, location)

    def minio_presigned_url(self, object_path: str) -> str:
        return generate_presigned_url(
            self.bucket_name, key=object_path, client=self.client
        )

    def minio_url(self, object_path: str) -> str:
        return minio_url(self._bucket().Object(object_path))

    def get_file(self, *, path: str, filename: str):
        try:
            return self._bucket().download_file(path, filename)
        except ClientError as e:
            raise RuntimeError(f"{e} path: {path}")

    def get_fileobj(self, *, path: str, file_obj):
        try:
            return self._bucket().download_fileobj(path, file_obj)
        except ClientError as e:
            raise RuntimeError(f"{e} path: {path}")

    def _get_via_temp_file(self, path: str, filename_to_data: Callable[[str], Any]):
        with tempfile.NamedTemporaryFile() as file:
            self.get_file(path=path, filename=file.name)
            return filename_to_data(file.name)

    def _put_via_temp_file(self, path: str, save_to_filename: Callable[[str], None]):
        with tempfile.NamedTemporaryFile() as file:
            save_to_filename(file.name)
            self.upload_file(file.name, path)

    def _get_gz(self, path: str, filename_to_data: Callable[[str], Any]):
        with tempfile.NamedTemporaryFile() as zipped_file:
            self.get_file(path=path, filename=zipped_file.name)
            with tempfile.NamedTemporaryFile() as file:
                open(file.name, "wb").write(
                    gzip.decompress(open(zipped_file.name, "rb").read())
                )
                return filename_to_data(file.name)

    def get_pandas_hdf(self, path: str) -> pd.DataFrame:
        return self._get_via_temp_file(path, pd.read_hdf)

    def put_pandas_hdf(self, path: str, data: pd.DataFrame):
        self._put_via_temp_file(
            path, lambda filename: data.to_hdf(filename, key="data")
        )

    def get_feather(self, path: str) -> pd.DataFrame:
        return self._get_via_temp_file(path, pd.read_feather)

    def put_feather(self, path: str, data):
        self._put_via_temp_file(path, lambda filename: data.to_feather(filename))

    def get_gz_feather(self, path: str):
        return self._get_gz(path, pd.read_feather)

    def put_gz_feather(self, path: str, data):
        with tempfile.NamedTemporaryFile() as file:
            data.to_feather(file.name)
            with tempfile.NamedTemporaryFile() as zipped_file:
                open(zipped_file.name, "wb").write(
                    gzip.compress(open(file.name, "rb").read())
                )
                self.upload_file(zipped_file.name, path)

    def get_pickle(self, path: str):
        return self._get_via_temp_file(
            path, lambda filename: pickle.load(open(filename, "rb"))
        )

    def put_pickle(self, path: str, data):
        self._put_via_temp_file(
            path, lambda filename: pickle.dump(data, open(filename, "wb"))
        )

    def get_text(self, path: str):
        return download_text(self._bucket().Object(path))

    def put_text(self, path: str, data):
        return self._bucket().Object(path).put(Body=data)

    def get_csv(self, path: str, **kwargs) -> pd.DataFrame:
        return self._get_via_temp_file(
            path, lambda filename: pd.read_csv(filename, **kwargs)
        )

    def get_gz_csv(self, path: str, **kwargs) -> pd.DataFrame:
        return self._get_gz(path, lambda filename: pd.read_csv(filename, **kwargs))

    def put_csv(self, path: str, data):
        self._put_via_temp_file(path, lambda filename: data.to_csv(filename))

    def get_parquet(self, path: str) -> pd.DataFrame:
        return self._get_via_temp_file(path, pd.read_parquet)

    def put_parquet(self, path: str, data: pd.DataFrame):
        self._put_via_temp_file(path, lambda filename: data.to_parquet(filename))

    def get_from_load_function(self, path: str, load_function=None):
        if load_function is None:
            from keras.models import load_model

            load_function = load_model
        with tempfile.NamedTemporaryFile(suffix=".keras") as file:
            self.get_file(path=path, filename=file.name)
            return load_function(file.name)

    def put_objsave(self, path: str, obj):
        with tempfile.NamedTemporaryFile(suffix=".keras") as file:
            obj.save(file.name)
            self.upload_file(file.name, path)

    def get_json(self, path: str) -> Any:
        return self._get_via_temp_file(path, lambda filename: json.load(open(filename, "r")))

    def put_json(self, path: str, data: Any):
        self._put_via_temp_file(path, lambda filename: json.dump(data, open(filename, "w")))


@dataclass()
class S3Details:
    key: str
    secret_key: str
    endpoint: str
    dashboard_url: str
    region_name: Optional[str] = None

    def create_resource(self) -> boto3.resource:
        return self._create_s3(boto3.resource)

    def create_client(self) -> boto3.client:
        return self._create_s3(boto3.client)

    def _create_s3(self, fcn):
        return fcn("s3", region_name=self.region_name, **self.to_s3_config_dict())

    def to_s3_config_dict(self):
        return dict(
            aws_access_key_id=self.key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.endpoint,
        )

    def bucket_actions(self, bucket_name: str) -> S3BucketActions:
        return S3BucketActions(
            resource=self.create_resource(),
            client=self.create_client(),
            bucket_name=bucket_name,
        )

    def shenron_cache_params(self, bucket_name: str) -> Dict[str, str]:
        return {
            "endpoint": self.endpoint,
            "bucket": bucket_name,
            "access_key": self.key,
            "secret": self.secret_key,
            "protocol": "s3",
        }

"""
Currently
"""

OPTIVER_BUCKET_DETAILS = S3Details(
    endpoint = "https://s3.ap-southeast-2.amazonaws.com",
    dashboard_url='',
    region_name = 'ap-southeast-2',
)


def exists(obj):
    try:
        # noinspection PyStatementEffect
        obj.content_length
        return True
    except ClientError:
        return False

def download_text(obj):
    return obj.get()["Body"].read().decode("utf-8")

def bucket_name_from_strategy(strategy):
    if strategy == 'optiver_hft_d1':
        return OPTIVER_BUCKET_DETAILS
    else:
        raise NotImplementedError()

def s3_details_from_strategy(strategy):
    if strategy == 'optiver_hft_d1':
        return OPTIVER_BUCKET_DETAILS
    else:
        return None
