"""Azure SQL MCP Server package."""

__version__ = "1.0.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .server import main, execute_query, get_tables, get_table_schema

__all__ = [
    "main",
    "execute_query",
    "get_tables",
    "get_table_schema"
]