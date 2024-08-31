"""Tests the client BigQueryStream."""

import unittest
from unittest import mock

import math
from typing import List
from sqlalchemy import create_mock_engine, engine, Inspector, Column
from sqlalchemy.types import String, Float

from singer_sdk._singerlib import Catalog, CatalogEntry
from tap_bigquery.tap import TapBigQuery
from tap_bigquery.client import BigQueryConnector, BigQueryStream

from tests.test_core import SAMPLE_CONFIG

def dump(sql, *multiparams, **params):
    print(sql.compile(dialect=engine.dialect))

class MockInspector(Inspector):
    def __init__(
        self,
        schema_names:List[str] = None,
        table_names:List[str] = None,
        table_columns = None,
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

    def get_indexes(self, table_name: str, schema: str) -> List[str]:
        return []

    def get_columns(self, table_name: str, schema: str) -> dict[str, Column]:
        return self.table_columns[schema + '.' + table_name]

    def get_pk_constraint(self, table_name: str, schema: str) -> dict:
        return {}


class TestClient(unittest.TestCase):
    """Test class for client tests."""

    mock_records = []

    def setUp(self):
        self.mock_config = SAMPLE_CONFIG
        # default catalog setup discovers streams
        self.mock_catalog = None

    @mock.patch("sqlalchemy.create_engine", return_value=create_mock_engine('bigquery://mockprojectid', dump))
    @mock.patch("sqlalchemy.inspect", return_value=MockInspector(
            ['mock-schema'], 
            ['mock_table'],
            {
                'mock-schema.mock_table': [
                    { 'name': 'float_infinity', 'type': Float },
                ],
            },
        )
    )
    def test_record_serialisable_post_processing(self, mock_engine, mock_inspector):
        # given a mock DB schema
        # given a mock DB table
        # given a tap instance with empty catalog
        tap = TapBigQuery(
            config=self.mock_config,
            catalog=self.mock_catalog,
        )
        # given an sqlachemy result set contains floats with infinity values
        self.mock_records.append({
            "float_infinity": math.inf,
        })
        # when post_process
        self.assertEqual(len(tap.streams), 1)

        # expect the result can be serialised by simplejson
        record = tap.streams['mock-schema-mock_table'].post_process(self.mock_records[0])
        json_output = BigQueryConnector().serialize_json(record)
        self.assertEqual(json_output, '{"float_infinity":null}')


    @mock.patch("sqlalchemy.create_engine", return_value=create_mock_engine('bigquery://mockprojectid', dump))
    @mock.patch("sqlalchemy.inspect", return_value=MockInspector(
            ['mock-schema'], 
            ['mock_table'],
            {
                'mock-schema.mock_table': [
                    { 'name': 'string_field', 'type': String(50) },
                    { 'name': 'float_field', 'type': Float },
                    { 'name': 'float_none', 'type': Float },
                    # { 'name': 'float_infinity', 'type': Float },
                ],
            },
        )
    )
    @mock.patch("singer_sdk.SQLStream.get_records", return_value=mock_records)
    def test_record_serialisable(self, mock_engine, mock_inspector, mock_tap_records):
        # given a mock DB schema
        # given a mock DB table
        # given a tap instance with catalog discovered from sqlalchemy
        tap = TapBigQuery(
            config=self.mock_config,
            catalog=self.mock_catalog,
        )
        # given an sqlachemy result set contains floats with None values
        self.mock_records.append({
            "string_field": "jam",
            "float_field": 0.0,
            "float_none": None,
            # "float_infinity": math.inf,
        })
        # when get_records
        self.assertEqual(len(tap.streams), 1)
        records = tap.streams['mock-schema-mock_table'].get_records(partition = None)

        # expect the result can be serialised by simplejson
        for record in records:
            json_output = BigQueryConnector().serialize_json(record)
            self.assertEqual(json_output, '{"string_field":"jam","float_field":0.0,"float_none":null}')


    @mock.patch("sqlalchemy.create_engine", return_value=create_mock_engine('bigquery://mockprojectid', dump))
    @mock.patch("sqlalchemy.inspect", return_value=MockInspector())
    @mock.patch("singer_sdk.SQLStream.get_records", return_value=mock_records)
    def test_catalog_supplied(self, mock_engine, mock_inspector, mock_sql_tap):
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
                                "float_field":{
                                    "type":[
                                        "float_value",
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
        self.mock_records.append({
            "string_field": "jam",
            "float_field": 0.0,
        })
        # when get_records
        self.assertEqual(len(tap.streams), 1)
        records = tap.streams['mock_table'].get_records(partition = None)

        # expect the result can be serialised by simplejson
        for record in records:
            json_output = BigQueryConnector().serialize_json(record)
            self.assertEqual(json_output, '{"string_field":"jam","float_field":0.0}')


    @mock.patch("sqlalchemy.create_engine", return_value=create_mock_engine('bigquery://mockprojectid', dump))
    @mock.patch("sqlalchemy.inspect", return_value=MockInspector(
            ['mock-schema'], 
            ['mock_table'],
            {
                'mock-schema.mock_table': [
                    { 'name': 'string_field', 'type': String(50) },
                ],
            },
        )
    )
    @mock.patch("singer_sdk.SQLStream.get_records", return_value=mock_records)
    def test_catalog_discovery(self, mock_engine, mock_inspector, mock_sql_tap):
        # given a mock DB schema
        # given a mock DB table
        # given a tap instance with catalog discovered from sqlalchemy
        tap = TapBigQuery(
            config=self.mock_config,
            catalog=self.mock_catalog,
        )
        # given an sqlachemy result set contains floats with None values
        self.mock_records.append({
            "string_field": "jam",
        })
        # when get_records
        self.assertEqual(len(tap.streams), 1)
        records = tap.streams['mock-schema-mock_table'].get_records(partition = None)

        # expect the result can be serialised by simplejson
        for record in records:
            json_output = BigQueryConnector().serialize_json(record)
            self.assertEqual(json_output, '{"string_field":"jam"}')
