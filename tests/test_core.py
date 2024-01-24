"""Tests standard tap features using the built-in SDK tests library."""

import json

from singer_sdk.testing import get_tap_test_class

from tap_bigquery.tap import TapBigQuery

# Run standard built-in tap tests from the SDK:
TestTapBigQuery = get_tap_test_class(
    tap_class=TapBigQuery,
    config=json.load(open(".secrets/config.json")),
)
