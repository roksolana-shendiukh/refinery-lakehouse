"""Shared helpers for the RAG tests."""

import re
from pathlib import Path

RAG_TABLES_TF = Path(__file__).parents[2] / "infra" / "rag_tables.tf"


def terraform_columns(table):
    """Column names of a databricks_sql_table resource in infra/rag_tables.tf."""
    resource = RAG_TABLES_TF.read_text(encoding="utf-8").split(f'resource "databricks_sql_table" "{table}"')[1]
    resource = resource.split('resource "databricks_sql_table"')[0]
    return re.findall(r'column \{\s*name\s*=\s*"(\w+)"', resource)
