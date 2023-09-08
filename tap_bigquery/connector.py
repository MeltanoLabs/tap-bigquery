"""A sample implementation for BigQuery."""

from __future__ import annotations

import sqlalchemy
from singer_sdk import SQLConnector
from singer_sdk import typing as th  # JSON schema typing helpers
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector


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
        if self.config.get("credentials_path"):
            return sqlalchemy.create_engine(
                self.sqlalchemy_url,
                echo=False,
                credentials_path=self.config.get("credentials_path"),
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
