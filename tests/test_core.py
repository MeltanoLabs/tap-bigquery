"""Tests standard tap features using the built-in SDK tests library."""

import unittest
from unittest import mock
from singer_sdk.testing import get_standard_tap_tests
from sqlalchemy import create_mock_engine, engine, Inspector

from tap_bigquery.tap import TapBigQuery
from tap_bigquery.connector import BigQueryConnector

SAMPLE_CONFIG = {
    "project_id": "",
    "filter_schemas": [],
    "filter_tables": [],
    "google_application_credentials": "MOCK",
}

def dump(sql, *multiparams, **params):
    print(sql.compile(dialect=engine.dialect))

class MockInspector(Inspector):
    def get_schema_names():
        return []

class TestCore(unittest.TestCase):
    """Test class for core tap tests."""

    def setUp(self):
        self.mock_config = SAMPLE_CONFIG

    # Run standard built-in tap tests from the SDK:
    @mock.patch("sqlalchemy.create_engine", return_value=create_mock_engine('bigquery://mockprojectid', dump))
    @mock.patch("sqlalchemy.inspect", return_value=MockInspector)
    def test_standard_tap_tests(self, mock_engine, mock_inspector):
        tests = get_standard_tap_tests(TapBigQuery, config=self.mock_config)
        for test in tests:
            test()
