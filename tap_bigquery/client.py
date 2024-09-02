"""SQL client handling.

This includes BigQueryStream and BigQueryConnector.
"""

from __future__ import annotations

import math
import logging
from typing import Any, Iterable

from singer_sdk import SQLStream
from singer_sdk.helpers import types

from tap_bigquery.connector import BigQueryConnector
LOGGER = logging.getLogger(__name__)

class BigQueryStream(SQLStream):
    """Stream class for BigQuery streams."""

    connector_class = BigQueryConnector

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
        yield from super().get_records(partition)
