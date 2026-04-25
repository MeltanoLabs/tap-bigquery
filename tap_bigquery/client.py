"""SQL client handling.

This includes BigQueryStream and BigQueryConnector.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING

from gcsfs import GCSFileSystem
from google.auth import default as default_credentials
from google.cloud import bigquery
from singer_sdk import SQLStream
from singer_sdk.helpers._batch import JSONLinesEncoding

from tap_bigquery.connector import BigQueryConnector

if TYPE_CHECKING:
    from singer_sdk.helpers import types

CREDENTIALS_STRING_ENV = "GOOGLE_APPLICATION_CREDENTIALS_STRING"
CREDENTIALS_PATH_ENV = "GOOGLE_APPLICATION_CREDENTIALS"


def _get_bigquery_client(config: dict, project_id: str | None = None) -> bigquery.Client:
    """Build a BigQuery client. Credential resolution order:

    1. config['google_application_credentials'] (JSON or path)
    2. GOOGLE_APPLICATION_CREDENTIALS_STRING env (JSON)
    3. GOOGLE_APPLICATION_CREDENTIALS env (path)
    4. Application Default Credentials (ADC) – workload identity, gcloud, etc.
    """
    import logging

    logger = logging.getLogger(__name__)
    credentials = config.get("google_application_credentials")
    if not credentials and os.environ.get(CREDENTIALS_STRING_ENV):
        credentials = os.environ[CREDENTIALS_STRING_ENV]
    if not credentials and os.environ.get(CREDENTIALS_PATH_ENV):
        return bigquery.Client.from_service_account_json(
            os.environ[CREDENTIALS_PATH_ENV],
            project=project_id,
        )

    if credentials:
        try:
            return bigquery.Client.from_service_account_info(
                json.loads(credentials) if isinstance(credentials, str) else credentials,
                project=project_id,
            )
        except (TypeError, json.decoder.JSONDecodeError) as e:
            logger.debug(
                "Credentials not valid JSON (trying as file path): %s", str(e)
            )
        return bigquery.Client.from_service_account_json(credentials, project=project_id)

    creds, default_project = default_credentials(
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(
        credentials=creds,
        project=project_id or default_project,
    )


class BigQueryStream(SQLStream):
    """Stream class for BigQuery streams."""

    connector_class = BigQueryConnector
    selected_by_default = True

    @cached_property
    def client(self):
        return _get_bigquery_client(
            self.config,
            project_id=self.config.get("project_id"),
        )

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
                self.logger.warning(
                    "Dropping unsupported value from '%s' -> '%s'",
                    str(_keychain),
                    str(key),
                )
                del _dict[key]
            elif isinstance(value, list):
                new_array = [tup for tup in value if not isinstance(tup, float) or not math.isinf(tup)]
                if len(_dict[key]) != len(new_array):
                    self.logger.warning(
                        "Dropping %s unsupported values from '%s'",
                        len(_dict[key]) - len(new_array),
                        str(_keychain[:] + [key]),
                    )
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

    def get_batch_config(self, config):
        return config.get("google_storage_bucket")

    def get_batches(self, bucket: str, context):
        destination_uri = f"gs://{bucket}/{self.fully_qualified_name}-*.json.gz"

        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = (
            bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
        )
        job_config.compression = bigquery.Compression.GZIP

        self.logger.info(
            "Running extract job from table '%s' to bucket '%s'",
            self.fully_qualified_name,
            bucket,
        )

        query = self._build_extract_query()
        self.logger.debug(query)

        extract_job = self.client.query(query)

        try:
            extract_job.result()  # Waits for job to complete.
        except:
            if extract_job.running():
                self.logger.info("Cancelling extract job")
                extract_job.cancel()
            raise

        self.logger.info(
            "Extract job completed in %ss",
            (extract_job.ended - extract_job.started).total_seconds(),
        )

        fs = GCSFileSystem("gs", token=self.client._credentials)  # noqa: SLF001

        tempdir = Path(tempfile.mkdtemp(prefix="tap-bigquery-"))

        self.logger.info("Downloading extract job files to '%s'", tempdir)

        try:
            fs.get(destination_uri, tempdir)
        finally:
            self.logger.info("Cleaning up files in bucket")
            fs.rm(destination_uri)

        files = list(tempdir.glob("*.json.gz"))

        self.logger.info(
            "Downloaded %d file(s): %s",
            len(files),
            [str(f) for f in files],
        )

        yield JSONLinesEncoding("gzip"), [f.as_uri() for f in files]

    def _build_extract_query(self):
        query = """
        EXPORT DATA
            OPTIONS (
                uri = 'gs://{bucket}/{table}-*.json.gz',
                format = 'JSON',
                compression='GZIP',
                overwrite = true
            )
        AS (
            SELECT {expressions}
            FROM {table}
        )
        """

        expressions = _generate_property_expressions(
            self.get_selected_schema()["properties"],
        )

        return query.format(
            bucket=self.config["google_storage_bucket"],
            table=self.fully_qualified_name,
            expressions=", ".join(expressions),
        )


def _generate_property_expressions(properties: dict, qualifier: str | None = None):
    for name, schema in properties.items():
        qualified_name = f"{qualifier}.{name}" if qualifier else name

        if "properties" in schema:
            struct = "STRUCT({expressions}) AS {name}"  # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
            expressions = _generate_property_expressions(
                schema["properties"],
                qualifier=qualified_name,
            )

            yield struct.format(expressions=", ".join(expressions), name=name)

        else:
            yield qualified_name
