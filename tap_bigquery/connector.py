"""A sample implementation for BigQuery."""

from __future__ import annotations

import fnmatch
import json
import os
import typing as t

import sqlalchemy
from singer_sdk import SQLConnector
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema
from sqlalchemy_bigquery import (
    ARRAY,
    FLOAT,
    FLOAT64,
    INT64,
    INTEGER,
    NUMERIC,
    STRING,
    STRUCT,
)

if t.TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector


class BigQueryConnector(SQLConnector):
    """Connects to the BigQuery SQL source."""

    @staticmethod
    def _normalize_table_name(schema_name: str, table_name: str) -> str:
        """Strip duplicated schema prefixes from table names."""
        schema_prefix = f"{schema_name}."
        if table_name.startswith(schema_prefix):
            return table_name[len(schema_prefix):]
        return table_name

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
        credentials: str | dict | None = self.config.get("google_application_credentials")
        if not credentials and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_STRING"):
            credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS_STRING"]
        if not credentials and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            return sqlalchemy.create_engine(
                self.sqlalchemy_url,
                echo=False,
                credentials_path=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
            )

        if credentials:
            try:
                # Note: json_serializer/json_deserializer commented out - not needed for
                # BigQuery dialect as it handles JSON types natively
                return sqlalchemy.create_engine(
                    self.sqlalchemy_url,
                    echo=False,
                    credentials_info=(
                        json.loads(credentials)
                        if isinstance(credentials, str)
                        else credentials
                    ),
                )
            except (TypeError, json.decoder.JSONDecodeError):
                self.logger.debug(
                    "Credentials not valid JSON (trying as file path)",
                )
                return sqlalchemy.create_engine(
                    self.sqlalchemy_url,
                    echo=False,
                    credentials_path=credentials,
                )
        # No credentials: use Application Default Credentials (ADC)
        return sqlalchemy.create_engine(self.sqlalchemy_url, echo=False)

    def to_array_type(
        self,
        sql_type: (
            str  # noqa: ANN401
            | sqlalchemy.types.TypeEngine
            | type[sqlalchemy.types.TypeEngine]
            | t.Any
        ),
    ) -> th.ArrayType:
        if (isinstance(sql_type, ARRAY)):
            if (isinstance(sql_type.item_type, STRING)):
                jsonschema = th.ArrayType(th.StringType)
            if (isinstance(sql_type.item_type, (INT64, INTEGER))):
                jsonschema = th.ArrayType(th.NumberType)
            if (isinstance(sql_type.item_type, (FLOAT, FLOAT64, NUMERIC))):
                jsonschema = th.ArrayType(th.NumberType)
            if (isinstance(sql_type.item_type, NUMERIC)):
                jsonschema = th.ArrayType(th.NumberType)
            if (isinstance(sql_type.item_type, STRUCT)):
                properties = self.struct_to_properties(sql_type.item_type)
                jsonschema = th.ArrayType(
                    th.ObjectType(
                        *properties,
                    ),
                )
            return jsonschema

    def struct_to_properties(
        self,
        sql_type: (
            str  # noqa: ANN401
            | sqlalchemy.types.TypeEngine
            | type[sqlalchemy.types.TypeEngine]
            | t.Any
        ),
    ) -> dict:
        properties = []
        if (isinstance(sql_type, STRUCT)):
            for name, type_ in sql_type._STRUCT_fields:
                self.logger.debug("%s: property type: %s", name, type_)
                if (isinstance(type_, STRING)):
                    properties.append(th.Property(name, th.StringType))
                if (isinstance(type_, (INT64, INTEGER))):
                    properties.append(th.Property(name, th.IntegerType))
                if (isinstance(type_, (FLOAT, FLOAT64, NUMERIC))):
                    properties.append(th.Property(name, th.NumberType))
                if (isinstance(type_, STRUCT)):
                    properties.append(th.Property(name, th.ObjectType(*self.struct_to_properties(type_))))
                if (isinstance(type_, ARRAY)):
                    properties.append(th.Property(name, self.to_array_type(type_)))
        return properties

    def to_jsonschema_type(
        self,
        column_name,
        sql_type: (
            str  # noqa: ANN401
            | sqlalchemy.types.TypeEngine
            | type[sqlalchemy.types.TypeEngine]
            | t.Any
        ),
    ) -> dict:
        self.logger.debug(
            "%s: Type %s, Array: %s, Tuple: %s",
            column_name,
            sql_type,
            sql_type._is_array,
            sql_type._is_tuple_type,
        )
        if (isinstance(sql_type, ARRAY)):
            jsonschema = self.to_array_type(sql_type)
            return jsonschema.type_dict
        if (isinstance(sql_type, STRUCT)):
            properties = self.struct_to_properties(sql_type)
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
        table_name = self._normalize_table_name(schema_name, table_name)

        # Initialize unique stream name
        unique_stream_id = f"{schema_name}-{table_name}"

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
            jsonschema_type: dict = self.to_jsonschema_type(column_name, column_def["type"])
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

    def discover_catalog_entries(
        self,
        *args,
        **kwargs,
    ) -> list[dict[str, t.Any]]:
        """Discover catalog entries using per-table reflection.

        BigQuery's SQLAlchemy dialect can return table names prefixed with the
        dataset, and bulk reflection may then resolve dataset names as project
        IDs. Discovering tables one-by-one avoids that path and keeps the
        project ID stable.
        """
        del args, kwargs  # Connector discovery does not require passthrough args.
        engine = self._engine
        inspected = sqlalchemy.inspect(engine)

        catalog_entries: list[dict[str, t.Any]] = []
        for schema_name in self.get_schema_names(engine, inspected):
            for table_name, is_view in self.get_object_names(
                engine,
                inspected,
                schema_name,
            ):
                normalized_table_name = self._normalize_table_name(
                    schema_name,
                    table_name,
                )
                entry_dict = self.discover_catalog_entry(
                    engine=engine,
                    inspected=inspected,
                    schema_name=schema_name,
                    table_name=normalized_table_name,
                    is_view=is_view,
                ).to_dict()
                self._ensure_selected_by_default(entry_dict)
                catalog_entries.append(entry_dict)

        self.logger.info(
            "Discovered %d BigQuery stream(s): %s",
            len(catalog_entries),
            [entry.get("tap_stream_id") for entry in catalog_entries],
        )
        return catalog_entries

    @staticmethod
    def _ensure_selected_by_default(catalog_entry: dict[str, t.Any]) -> None:
        """Force stream-level selected-by-default metadata on discovered entries."""
        metadata_entries = catalog_entry.get("metadata", [])
        for metadata_entry in metadata_entries:
            if metadata_entry.get("breadcrumb") == []:
                metadata_entry.setdefault("metadata", {})["selected-by-default"] = True
                return

        metadata_entries.append(
            {
                "breadcrumb": [],
                "metadata": {"selected-by-default": True},
            },
        )
        catalog_entry["metadata"] = metadata_entries
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

        objects = [
            (self._normalize_table_name(schema_name, table_name), is_view)
            for (table_name, is_view) in super().get_object_names(
                engine,
                inspected,
                schema_name,
            )
        ]
        if table_patterns := self.config.get("filter_tables"):
            return [
                o
                for o in objects
                if any(fnmatch.fnmatch(o[0], p) for p in table_patterns)
            ]

        return objects

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
