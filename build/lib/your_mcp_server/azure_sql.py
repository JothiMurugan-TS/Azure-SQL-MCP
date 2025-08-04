
import os
import pyodbc
import logging
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("your_mcp_server.azure_sql")

class AzureSQLConnector:
    def __init__(self):
        self.connection_string = None
        self.setup_connection()

    def setup_connection(self):
        """Setup Azure SQL connection string"""
        server_name = os.getenv("AZURE_SQL_SERVER")
        database = os.getenv("AZURE_SQL_DATABASE")
        username = os.getenv("AZURE_SQL_USERNAME")
        password = os.getenv("AZURE_SQL_PASSWORD")
        print(server_name, database, username, password)

        self.connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )

    def get_connection(self):
        """Get database connection"""
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
