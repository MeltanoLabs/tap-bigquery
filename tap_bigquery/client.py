"""SQL client handling.

This includes BigQueryStream and BigQueryConnector.
"""

from __future__ import annotations

from typing import Any, Iterable

from singer_sdk import SQLStream

from tap_bigquery.connector import BigQueryConnector


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
        yield from super().get_records(partition)
