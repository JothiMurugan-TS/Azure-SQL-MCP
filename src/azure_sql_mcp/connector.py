import logging
import os
import pyodbc
from typing import Optional

logger = logging.getLogger(__name__)

class AzureSQLConnector:
    def __init__(self):
        self.connection_string: Optional[str] = None
        self.setup_connection()

    def setup_connection(self):
        """Setup Azure SQL connection string"""
        server_name = os.getenv("AZURE_SQL_SERVER")
        database = os.getenv("AZURE_SQL_DATABASE")
        username = os.getenv("AZURE_SQL_USERNAME")
        password = os.getenv("AZURE_SQL_PASSWORD")
        
        if not all([server_name, database, username, password]):
            raise ValueError("Missing required environment variables for Azure SQL connection")
        
        logger.info(f"Connecting to server: {server_name}, database: {database}")
        
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
        if not self.connection_string:
            raise ValueError("Connection string not configured")
        
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
