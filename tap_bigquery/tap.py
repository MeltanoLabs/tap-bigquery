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
            description="GCP Project",
        ),
        th.Property(
            "credentials_path",
            th.StringType,
            required=False,
            description="The path to the service account credentials file.",
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
    ).to_dict()

    default_stream_class: type[SQLStream] = BigQueryStream


if __name__ == "__main__":
    TapBigQuery.cli()
