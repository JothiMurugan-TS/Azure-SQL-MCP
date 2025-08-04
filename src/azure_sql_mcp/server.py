import asyncio
import json
import logging
import sys
from typing import Any, Sequence
from dotenv import load_dotenv

from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.types import Tool, TextContent, LoggingLevel

from .connector import AzureSQLConnector

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create server instance
server = Server("azure-sql-mcp")

# Create connector instance
connector = AzureSQLConnector()

@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    """List available tools"""
    return [
        Tool(
            name="execute_query",
            description="Execute a SQL query on Azure SQL Database",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SQL query to execute"
                    },
                    "parameters": {
                        "type": "array",
                        "description": "Query parameters",
                        "items": {"type": "string"},
                        "default": []
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_tables",
            description="Get list of tables in the database",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="get_table_schema",
            description="Get schema information for a specific table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table"
                    }
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="create_table",
            description="Create a new table in the database",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to create"
                    },
                    "columns": {
                        "type": "string",
                        "description": "Column definitions (e.g., 'id INT PRIMARY KEY, name VARCHAR(100)')"
                    }
                },
                "required": ["table_name", "columns"]
            }
        ),
        Tool(
            name="insert_data",
            description="Insert data into a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table"
                    },
                    "columns": {
                        "type": "array",
                        "description": "Column names",
                        "items": {"type": "string"}
                    },
                    "values": {
                        "type": "array",
                        "description": "Values to insert",
                        "items": {"type": "string"}
                    }
                },
                "required": ["table_name", "columns", "values"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict | None) -> list[TextContent]:
    """Handle tool calls"""
    if arguments is None:
        arguments = {}
    
    try:
        if name == "execute_query":
            return await execute_query(arguments)
        elif name == "get_tables":
            return await get_tables()
        elif name == "get_table_schema":
            return await get_table_schema(arguments)
        elif name == "create_table":
            return await create_table(arguments)
        elif name == "insert_data":
            return await insert_data(arguments)
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as e:
        logger.error(f"Tool execution failed: {e}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]

# [Keep all your existing tool implementation functions here]
async def execute_query(arguments: dict) -> list[TextContent]:
    """Execute SQL query"""
    query = arguments.get("query", "")
    parameters = arguments.get("parameters", [])
    
    try:
        with connector.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, parameters)
            
            if query.strip().upper().startswith("SELECT"):
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # Format results as JSON
                data = []
                for row in results:
                    data.append(dict(zip(columns, row)))
                
                return [TextContent(
                    type="text",
                    text=f"Query executed successfully.\nResults:\n{json.dumps(data, indent=2, default=str)}"
                )]
            else:
                conn.commit()
                return [TextContent(
                    type="text",
                    text="Query executed successfully."
                )]
    except Exception as e:
        return [TextContent(type="text", text=f"Query execution failed: {str(e)}")]

async def get_tables() -> list[TextContent]:
    """Get list of tables"""
    try:
        with connector.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            return [TextContent(
                type="text",
                text=f"Tables in database:\n{json.dumps(tables, indent=2)}"
            )]
    except Exception as e:
        return [TextContent(type="text", text=f"Failed to get tables: {str(e)}")]

async def get_table_schema(arguments: dict) -> list[TextContent]:
    """Get table schema"""
    table_name = arguments.get("table_name", "")
    
    try:
        with connector.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """, table_name)
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "column_name": row[0],
                    "data_type": row[1],
                    "is_nullable": row[2],
                    "default_value": row[3]
                })
            
            return [TextContent(
                type="text",
                text=f"Schema for table '{table_name}':\n{json.dumps(columns, indent=2)}"
            )]
    except Exception as e:
        return [TextContent(type="text", text=f"Failed to get schema: {str(e)}")]

async def create_table(arguments: dict) -> list[TextContent]:
    """Create table"""
    table_name = arguments.get("table_name", "")
    columns = arguments.get("columns", "")
    
    try:
        with connector.get_connection() as conn:
            cursor = conn.cursor()
            query = f"CREATE TABLE {table_name} ({columns})"
            cursor.execute(query)
            conn.commit()
            
            return [TextContent(
                type="text",
                text=f"Table '{table_name}' created successfully."
            )]
    except Exception as e:
        return [TextContent(type="text", text=f"Failed to create table: {str(e)}")]

async def insert_data(arguments: dict) -> list[TextContent]:
    """Insert data into table"""
    table_name = arguments.get("table_name", "")
    columns = arguments.get("columns", [])
    values = arguments.get("values", [])
    
    try:
        with connector.get_connection() as conn:
            cursor = conn.cursor()
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["?" for _ in values])
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            cursor.execute(query, values)
            conn.commit()
            
            return [TextContent(
                type="text",
                text=f"Data inserted into '{table_name}' successfully."
            )]
    except Exception as e:
        return [TextContent(type="text", text=f"Failed to insert data: {str(e)}")]

async def main():
    """Main entry point"""
    from mcp.server.stdio import stdio_server
    
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="azure-sql-mcp",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())
