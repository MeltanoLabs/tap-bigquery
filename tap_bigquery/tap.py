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
            th.StringType,
            required=True,
            secret=True,
            description="JSON content or path to service account credentials.",
        ),
        th.Property(
            "filter_schemas",
            th.ArrayType(th.StringType),
            required=False,
            description=(
                "If an array of schema names is provided, the tap will only process the"
                " specified BigQuery schemas and ignore others. If left blank, the tap "
                "automatically determines ALL available BigQuery schemas."
            ),
        ),
        th.Property(
            "filter_datasets",
            th.ArrayType(th.StringType),
            required=False,
            description=(
                "If an array of dataset names is provided, the tap will only process the"
                " specified BigQuery datasets and ignore others. If left blank, the tap "
                "automatically determines ALL available BigQuery datasets."
            ),
        ),
    ).to_dict()

    default_stream_class: type[SQLStream] = BigQueryStream


if __name__ == "__main__":
    TapBigQuery.cli()
