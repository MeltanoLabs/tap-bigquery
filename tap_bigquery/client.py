"""SQL client handling.

This includes BigQueryStream and BigQueryConnector.
"""

from __future__ import annotations
from datetime import datetime

from typing import Any, Iterable

from singer_sdk import SQLStream

from tap_bigquery.connector import BigQueryConnector


def transform_record(dict) -> dict:
    # easy interface point to transform what we get from BigQuery before it gets transformed into Singer Spec
    for key, value in dict.items():
        if isinstance(value, str):
            try:  # ISO8601 is poorly handled and is mis-coerced by the SDK to `date`, not the correct `datetime`. So, we attempt to do it more explicitly here.
                dict[key] = datetime.fromisoformat(value)
            except:
                ...  # just means it wasn't an ISO timestamp, no big deal
    return dict

class BigQueryStream(SQLStream):
    """Stream class for BigQuery streams."""

    connector_class = BigQueryConnector

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
        yield [transform_record(record) for record in super().get_records(partition)]
