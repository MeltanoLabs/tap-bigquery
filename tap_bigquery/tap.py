"""BigQuery tap class."""

from __future__ import annotations

from singer_sdk import SQLStream, SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_bigquery.client import BigQueryStream


class TapBigQuery(SQLTap):
    """Google BigQuery tap."""

    name = "tap-bigquery"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "project_id",
            th.StringType,
            required=True,
            secret=False,
            description="GCP Project",
        ),
        th.Property(
            "google_application_credentials",
            th.OneOf(
                th.StringType,
                th.ObjectType(),
            ),
            required=True,
            secret=True,
            description="JSON content or path to service account credentials.",
        ),
        th.Property(
            "google_storage_bucket",
            th.StringType,
            required=False,
            secret=False,
            description="An optional Google Storage Bucket, when supplied a file based extract will be used.",
        ),
        th.Property(
            "filter_schemas",
            th.ArrayType(th.StringType),
            required=False,
            description=(
                "If an array of schema names is provided, the tap will only process "
                "the specified BigQuery schemas (datasets) and ignore others. If left "
                " blank, the tap automatically determines ALL available schemas."
            ),
        ),
        th.Property(
            "filter_tables",
            th.ArrayType(th.StringType),
            required=False,
            description=(
                "If an array of table names is provided, the tap will only process "
                "the specified BigQuery tables and ignore others. If left blank, the "
                "tap automatically determines ALL available tables. Shell patterns are "
                "supported."
            ),
        ),
    ).to_dict()

    default_stream_class: type[SQLStream] = BigQueryStream


if __name__ == "__main__":
    TapBigQuery.cli()
