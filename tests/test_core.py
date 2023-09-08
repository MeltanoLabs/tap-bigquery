"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_bigquery.tap import TapBigQuery

SAMPLE_CONFIG = {
    "project_id": "",
    "filter_schemas": [],
    "credentials_path": ","
}

# Run standard built-in tap tests from the SDK:
TestTapBigQuery = get_tap_test_class(
    tap_class=TapBigQuery,
    config=SAMPLE_CONFIG,
)
