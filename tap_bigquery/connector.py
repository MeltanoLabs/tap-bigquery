"""A sample implementation for BigQuery."""

from __future__ import annotations

import sqlalchemy
import json
import logging
from singer_sdk import SQLConnector
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema
from singer_sdk import typing as th  # JSON schema typing helpers
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy_bigquery import STRUCT
LOGGER = logging.getLogger(__name__)

class BigQueryConnector(SQLConnector):
    """Connects to the BigQuery SQL source."""

    def create_engine(self) -> Engine:
        """Creates and returns a new engine. Do not call outside of _engine.

        NOTE: Do not call this method. The only place that this method should
        be called is inside the self._engine method. If you'd like to access
        the engine on a connector, use self._engine.

        This method exists solely so that tap/target developers can override it
        on their subclass of SQLConnector to perform custom engine creation
        logic.

        Returns:
            A new SQLAlchemy Engine.
        """
        if self.config.get("google_application_credentials"):
            try:
                credentials_info=json.loads(self.config.get("google_application_credentials"))
                return sqlalchemy.create_engine(
                    self.sqlalchemy_url,
                    echo=False,
                    credentials_info=credentials_info,
                    # json_serializer=self.serialize_json,
                    # json_deserializer=self.deserialize_json,
                )
            except (TypeError, json.decoder.JSONDecodeError):
                LOGGER.warn("'google_application_credentials' not valid json")
                return sqlalchemy.create_engine(
                    self.sqlalchemy_url,
                    echo=False,
                    credentials_path=self.config.get("google_application_credentials"),
                    # json_serializer=self.serialize_json,
                    # json_deserializer=self.deserialize_json,
                )
        else:
            return sqlalchemy.create_engine(
                self.sqlalchemy_url,
                echo=False,
                # json_serializer=self.serialize_json,
                # json_deserializer=self.deserialize_json,
            )

    def to_jsonschema_type(
        self,
        sql_type,
    ) -> dict:
        LOGGER.debug("Type %s %s", sql_type, isinstance(sql_type, STRUCT))
        if (isinstance(sql_type, STRUCT)):
            properties = []
            for name, type_ in sql_type._STRUCT_fields:
                properties.append(th.Property(name, th.StringType))
            jsonschema = th.ObjectType(
                *properties,
            )
            return jsonschema.type_dict
        return super().to_jsonschema_type(sql_type)

    # TODO this only needs a column filtering capability in the singer-sdk
    # as sqlalchemy returns additional columns on bigquery for all the json
    # it has natively understood.
    def discover_catalog_entry(
        self,
        engine: Engine,  # noqa: ARG002
        inspected: Inspector,
        schema_name: str,
        table_name: str,
        is_view: bool,  # noqa: FBT001
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table or a view.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect
            table_name: Name of the table or a view
            is_view: Flag whether this object is a view, returned by `get_object_names`

        Returns:
            `CatalogEntry` object for the given table or a view
        """
        # Initialize unique stream name
        unique_stream_id = self.get_fully_qualified_name(
            db_name=None,
            schema_name=schema_name,
            table_name=table_name,
            delimiter="-",
        )

        # Detect key properties
        possible_primary_keys: list[list[str]] = []
        pk_def = inspected.get_pk_constraint(table_name, schema=schema_name)
        if pk_def and "constrained_columns" in pk_def:
            possible_primary_keys.append(pk_def["constrained_columns"])

        # An element of the columns list is ``None`` if it's an expression and is
        # returned in the ``expressions`` list of the reflected index.
        possible_primary_keys.extend(
            index_def["column_names"]  # type: ignore[misc]
            for index_def in inspected.get_indexes(table_name, schema=schema_name)
            if index_def.get("unique", False)
        )

        key_properties = next(iter(possible_primary_keys), None)

        # Initialize columns list
        table_schema = th.PropertiesList()
        for column_def in inspected.get_columns(table_name, schema=schema_name):
            column_name = column_def["name"]
            is_nullable = column_def.get("nullable", False)
            jsonschema_type: dict = self.to_jsonschema_type(column_def["type"])
            if ("." not in column_name):
                table_schema.append(
                    th.Property(
                        name=column_name,
                        wrapped=th.CustomType(jsonschema_type),
                        nullable=is_nullable,
                        required=column_name in key_properties if key_properties else False,
                    ),
                )
        schema = table_schema.to_dict()

        # Initialize available replication methods
        addl_replication_methods: list[str] = [""]  # By default an empty list.
        # Notes regarding replication methods:
        # - 'INCREMENTAL' replication must be enabled by the user by specifying
        #   a replication_key value.
        # - 'LOG_BASED' replication must be enabled by the developer, according
        #   to source-specific implementation capabilities.
        replication_method = next(reversed(["FULL_TABLE", *addl_replication_methods]))

        # Create the catalog entry object
        return CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=table_name,
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            is_view=is_view,
            replication_method=replication_method,
            metadata=MetadataMapping.get_standard_metadata(
                schema_name=schema_name,
                schema=schema,
                replication_method=replication_method,
                key_properties=key_properties,
                valid_replication_keys=None,  # Must be defined by user
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        return f"bigquery://{config['project_id']}"

    def get_object_names(
        self,
        engine,
        inspected,
        schema_name: str,
    ) -> list[tuple[str, bool]]:
        """Return discoverable object names."""
        # Bigquery inspections returns table names in the form
        # `schema_name.table_name` which later results in the project name
        # override due to specifics in behavior of sqlalchemy-bigquery
        #
        # Let's strip `schema_name` prefix on the inspection

        return [
            (table_name.split(".")[-1], is_view)
            for (table_name, is_view) in super().get_object_names(
                engine,
                inspected,
                schema_name,
            )
        ]

    def get_schema_names(self, engine: Engine, inspected: Inspector) -> list[str]:
        """Return a list of schema names in DB, or overrides with user-provided values.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine

        Returns:
            List of schema names
        """
        if "filter_schemas" in self.config and len(self.config["filter_schemas"]) != 0:
            return self.config["filter_schemas"]
        return super().get_schema_names(engine, inspected)

__all__ = ["TapBigQuery", "BigQueryConnector", "BigQueryStream"]
