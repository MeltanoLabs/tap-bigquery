"""Tests the client BigQueryStream."""

from typing import List
import unittest
from unittest import mock
from sqlalchemy import create_mock_engine, engine, Inspector, Column
from sqlalchemy.types import String

from singer_sdk._singerlib import Catalog, CatalogEntry
from tap_bigquery.tap import TapBigQuery
from tap_bigquery.client import BigQueryConnector

from tests.test_core import SAMPLE_CONFIG

def dump(sql, *multiparams, **params):
    print(sql.compile(dialect=engine.dialect))

class MockInspector(Inspector):
    def __init__(
        self,
        schema_names,
        table_names,
        table_columns
    ):
        self.schema_names = schema_names
        self.table_names = table_names
        self.table_columns = table_columns

    def get_schema_names(self)-> List[str]:
        return self.schema_names
    
    def get_table_names(self, schema:str) -> List[str]:
        return self.table_names
    
    def get_view_names(self, schema:str) -> List[str]:
        raise NotImplementedError
    
    def get_columns(self, table_name: str, schema_name: str) -> dict[str, Column]:
        return self.table_columns[schema_name + '.' + table_name]


class TestClient(unittest.TestCase):
    """Test class for client tests."""

    mock_records = []

    def setUp(self):
        self.mock_config = SAMPLE_CONFIG
        # default catalog setup discovers streams
        self.mock_catalog = None

    # Run standard built-in tap tests from the SDK:
    @mock.patch("sqlalchemy.create_engine", return_value=create_mock_engine('bigquery://mockprojectid', dump))
    @mock.patch("sqlalchemy.inspect", return_value=MockInspector(
            'mock-schema', 
            'mock_table',
            {
                'mock-schema.mock_table': [
                    {
                        'name': 'string_field',
                        'type': String(50),
                    }
                ],
            }
        )
    )
    @mock.patch("singer_sdk.SQLStream.get_records", return_value=mock_records)
    def test_record_with_none_float_serialisable(self, mock_engine, mock_inspector, mock_sql_tap):
        # given a mock DB schema
        # given a mock DB table
        # given a tap instance with catalog defining one dataset
        catalog_dict = {
            "streams": 
                CatalogEntry.from_dict(
                    {
                        "tap_stream_id": "mock_table",
                        "table_name": "mock-schema.mock_table",
                        "replication_method": "",
                        "key_properties": [],
                        "schema": {
                            "properties":{
                                "string_field":{
                                    "type":[
                                        "string",
                                        "null",
                                    ]
                                },
                            },
                        },
                    }
                )
        }
        self.mock_catalog = Catalog(catalog_dict)
        tap = TapBigQuery(
            config=self.mock_config,
            catalog=self.mock_catalog,
        )
        # given an sqlachemy result set contains floats with None values
        self.mock_records.append({"string_field": "jam"})
        # when get_records
        self.assertEqual(len(tap.streams), 1)
        records = tap.streams['mock_table'].get_records(partition = None)

        # expect the result can be serialised by simplejson
        for record in records:
            json_output = BigQueryConnector().serialize_json(record)
            self.assertEqual(json_output, '{"string_field":"jam"}')
            


