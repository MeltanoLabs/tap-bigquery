"""SQL client handling.

This includes BigQueryStream and BigQueryConnector.
"""

from __future__ import annotations

import json
import logging
import math
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

import fsspec
from google.cloud import bigquery
from singer_sdk import SQLStream
from singer_sdk.helpers._batch import JSONLinesEncoding, SDKBatchMessage

from tap_bigquery.connector import BigQueryConnector

if TYPE_CHECKING:
    from gcsfs import GCSFileSystem
    from singer_sdk.helpers import types

LOGGER = logging.getLogger(__name__)

class BigQueryStream(SQLStream):
    """Stream class for BigQuery streams."""

    connector_class = BigQueryConnector

    def get_bigquery_client(self):
        """Initialize a bigquery client from credentials json or path.

        Returns:
            Initialized BigQuery client.
        """
        if self.config.get("google_application_credentials"):
            try:
                return bigquery.Client.from_service_account_info(json.loads(self.config.get("google_application_credentials")))
            except (TypeError, json.decoder.JSONDecodeError):
                LOGGER.warning("'google_application_credentials' not valid json trying path")
                return bigquery.Client.from_service_account_json(self.config.get("google_application_credentials"))

    def prepare_serialisation(self, _dict, _keychain = []):
        """
        Fix 'ValueError: Out of range float values are not JSON compliant'
        Recursively delete keys with the value ``None`` in a dictionary.
        Recursively delete keys with the value ``math.inf`` in a dictionary.
        NB - This alters the input so the return is just a convenience.
        """
        for key, value in list(_dict.items()):
            if isinstance(value, dict):
                self.prepare_serialisation(value, _keychain[:] + [key])
            # elif value is None:
            #     del _dict[key]
            elif isinstance(value, float) and math.isinf(value):
                LOGGER.warning("Dropping unsupported value from '%s' -> '%s'", str(_keychain), str(key))
                del _dict[key]
            elif isinstance(value, list):
                new_array = [tup for tup in value if not isinstance(tup, float) or not math.isinf(tup)]
                if len(_dict[key]) != len(new_array):
                    LOGGER.warning("Dropping %s unsupported values from '%s'", len(_dict[key]) - len(new_array), str(_keychain[:] + [key]))
                    _dict[key] = new_array
                for v_i in value:
                    if isinstance(v_i, dict):
                        self.prepare_serialisation(v_i, _keychain[:] + [key])
        return _dict

    def post_process(  # noqa: PLR6301
        self,
        row: types.Record,
        context: types.Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        return self.prepare_serialisation(row)

    def get_records(self, partition: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Optionally, add custom logic instead of calling the super().
        # This is helpful if the source database provides batch-optimized record
        # retrieval.
        # If no overrides or optimizations are needed, you may delete this method.
        if bucket := self.config.get("google_storage_bucket"):
            client = self.get_bigquery_client()
            destination_uri = f"gs://{bucket}/{self.fully_qualified_name}-*.json.gz"

            job_config = bigquery.ExtractJobConfig()
            job_config.destination_format = (
                bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
            )
            job_config.compression = bigquery.Compression.GZIP

            LOGGER.info(
                "Running extract job from table '%s' to bucket '%s'",
                self.fully_qualified_name,
                bucket,
            )

            extract_job = client.extract_table(
                self.fully_qualified_name,
                destination_uri,
                job_config=job_config,
            )
            extract_job.result()  # Waits for job to complete.

            LOGGER.info(
                "Extract job completed in %ss",
                (extract_job.ended - extract_job.started).total_seconds(),
            )

            # emit batch or separate records (needs config for batch e.g. 'target_supports_batch_messages')
            fs: GCSFileSystem = fsspec.filesystem("gs", token=client._credentials)  # noqa: SLF001

            tempdir = Path(tempfile.mkdtemp(prefix="tap-bigquery-"))

            LOGGER.info("Downloading extract job files to '%s'", tempdir)

            try:
                fs.get(destination_uri, tempdir)
            finally:
                LOGGER.info("Cleaning up files in bucket")
                fs.rm(destination_uri)

            files = list(tempdir.glob("*.json.gz"))

            LOGGER.info(
                "Downloaded %d file(s): %s",
                len(files),
                [str(f) for f in files],
            )

            self._write_batch_message(
                JSONLinesEncoding("gzip"),
                [f.as_uri() for f in files],
            )

            return None

        yield from super().get_records(partition)

    def _write_batch_message(self, encoding, manifest):
        for stream_map in self.stream_maps:
            self._tap.write_message(
                SDKBatchMessage(
                    stream=stream_map.stream_alias,
                    encoding=encoding,
                    manifest=manifest,
                ),
            )
        self._is_state_flushed = False
