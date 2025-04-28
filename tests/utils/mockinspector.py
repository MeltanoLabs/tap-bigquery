from __future__ import annotations

from typing import List
from sqlalchemy import Inspector, Column

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