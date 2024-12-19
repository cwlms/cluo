from types import TracebackType
from typing import Type, Optional, List, Set
import boto3
from botocore.client import Config
import mimetypes
from pathlib import Path

from cluo.config import RunMode, config_state
from cluo.core import (
    Batch,
    ErrorHandlingMethod,
    OffsetManager,
    PossibleOffsetType,
    Record,
    Source,
)

class S3Source(Source):
    """Returns batches of data from Amazon S3 storage."""

    def __init__(
        self,
        bucket: str,
        prefix: str,
        extensions: Optional[List[str]] = None,
        mimetypes: Optional[List[str]] = None,
        aws_key: Optional[str] = None,
        aws_secret_key: Optional[str] = None,
        region_name: str = "us-east-1",
        batch_size: Optional[int] = 10,
        name: Optional[str] = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        offset_manager: Optional[OffsetManager] = None,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `S3Source`.

        Args:
            bucket (str): The S3 bucket name.
            prefix (str): The prefix/folder path to read from.
            extensions (List[str], optional): List of allowed file extensions (e.g., ['.csv', '.json']).
                Case insensitive. If None, all extensions are allowed.
            mimetypes (List[str], optional): List of allowed MIME types (e.g., ['text/csv', 'application/json']).
                If None, all MIME types are allowed.
            aws_key (str, optional): AWS access key. Defaults to None (uses environment/IAM).
            aws_secret_key (str, optional): AWS secret key. Defaults to None (uses environment/IAM).
            region_name (str, optional): AWS region. Defaults to "us-east-1".
            batch_size (int, optional): Number of objects to include in each batch. Defaults to 10.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): How to handle errors.
            offset_manager (OffsetManager, optional): Offset manager for tracking progress.
            expose_metrics (bool, optional): Whether to expose metrics. Defaults to False.
        """
        Source.__init__(
            self,
            batch_size=batch_size,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.source_offset_manager = offset_manager
        self.current_offset: PossibleOffsetType = None
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name,
            config=Config(signature_version="s3v4")
        )
        self._object_list = []
        self._current_index = 0

        # Initialize file type filtering
        self.extensions: Set[str] = {
            ext.lower() for ext in (extensions or [])
        }
        self.mimetypes: Set[str] = {
            mime.lower() for mime in (mimetypes or [])
        }

    def _is_allowed_file(self, key: str) -> bool:
        """Check if the file matches the allowed extensions and MIME types.

        Args:
            key (str): The S3 object key to check.

        Returns:
            bool: True if the file is allowed, False otherwise.
        """
        # If no filters are set, allow all files
        if not self.allowed_extensions and not self.allowed_mime_types:
            return True

        # Check file extension
        if self.allowed_extensions:
            file_ext = Path(key).suffix.lower()
            if file_ext not in self.allowed_extensions:
                return False

        # Check MIME type
        if self.allowed_mime_types:
            mime_type, _ = mimetypes.guess_type(key)
            if mime_type is None or mime_type.lower() not in self.allowed_mime_types:
                return False

        return True

    def _list_objects(self) -> None:
        """Lists all objects under the specified prefix."""
        paginator = self.s3_client.get_paginator("list_objects_v2")
        
        # Start from the current offset if it exists
        kwargs = {
            "Bucket": self.bucket,
            "Prefix": self.prefix
        }
        if self.current_offset:
            kwargs["StartAfter"] = self.current_offset

        for page in paginator.paginate(**kwargs):
            if "Contents" in page:
                self._object_list.extend(
                    [obj["Key"] for obj in page["Contents"]]
                )

    def process_batch(self, batch: Optional[Batch]) -> Batch:
        """Process and return the next batch of objects from S3."""
        records = []
        
        while len(records) < self.batch_size and self._current_index < len(self._object_list):
            key = self._object_list[self._current_index]
            try:
                response = self.s3_client.get_object(
                    Bucket=self.bucket,
                    Key=key
                )
                content = response["Body"].read()
                
                records.append(Record({
                    "key": key,
                    "content": content,
                    "last_modified": response["LastModified"],
                    "content_length": response["ContentLength"],
                    "content_type": response.get("ContentType"),
                }))
                
                self.current_offset = key
                
            except Exception as e:
                # Handle any errors based on error_handling_method
                if self.error_handling_method == ErrorHandlingMethod.SKIP:
                    continue
                else:
                    raise e
            
            self._current_index += 1
            
        return Batch(records)

    def __enter__(self) -> None:
        """Set up the source, including loading the initial offset if applicable."""
        if config_state.RUN_MODE == RunMode.WRITE:
            if self.source_offset_manager:
                self.source_offset_manager.set_key(self.unique_name)
                self.current_offset = self.source_offset_manager.get()
        
        # List all objects that we'll process
        self._list_objects()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        """Clean up and save the offset if necessary."""
        has_exception = (
            exc_type is not None or exc_value is not None or exc_tb is not None
        )
        if not has_exception and config_state.RUN_MODE == RunMode.WRITE:
            if self.current_offset:
                self.source_offset_manager.set(self.current_offset)
            return True
        return False