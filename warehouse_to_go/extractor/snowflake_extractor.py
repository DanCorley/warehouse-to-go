from typing import Dict, List, Optional, Any
import snowflake.connector
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
import duckdb
import tempfile
import os
import numpy as np
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from warehouse_to_go.utils.config import Config
from warehouse_to_go.utils.output import print_info, print_success, print_status, print_error

@dataclass
class ExtractionTask:
    database: str
    schema: str
    table: str
    columns: Optional[List[str]] = None
    row_limit: int = 10000
    batch_size: int = 10000

def test_connection(config: Config) -> None:
    """Test Snowflake connection using the provided configuration."""
    with SnowflakeExtractor(config) as extractor:
        extractor.test_connection()

class SnowflakeExtractor:
    """Class to handle data extraction from Snowflake."""
    
    def __init__(self, config: Config):
        """Initialize with configuration."""
        self.config = config
        self.conn = None
        
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.conn:
            self.conn.close()
            
    def test_connection(self) -> None:
        """Test the Snowflake connection."""
        self._get_connection().close()
            
    def _get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Get or create Snowflake connection."""
        if not self.conn:
            # Create connection
            conn_params = {
                'account': self.config.warehouse.account,
                'user': self.config.warehouse.user,
                'warehouse': self.config.warehouse.warehouse,
                'role': self.config.warehouse.role,
                'database': self.config.warehouse.database,
                'schema': self.config.warehouse.schema,
                'client_session_keep_alive': self.config.warehouse.client_session_keep_alive,
                'query_tag': self.config.warehouse.query_tag,
            }
            
            # Add authentication
            if self.config.warehouse.private_key_path:
                with open(self.config.warehouse.private_key_path, 'rb') as key:
                    p_key = serialization.load_pem_private_key(
                        key.read(),
                        password=self.config.warehouse.private_key_passphrase.encode() if self.config.warehouse.private_key_passphrase else None,
                        backend=default_backend()
                    )
                    
                # Convert to bytes in the format Snowflake expects
                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                conn_params['private_key'] = pkb
            elif self.config.warehouse.password:
                conn_params['password'] = self.config.warehouse.password
            else:
                raise ValueError("No authentication method provided")
            
            self.conn = snowflake.connector.connect(**conn_params)
            
        return self.conn
    
    def _convert_df_for_duckdb(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert DataFrame to types compatible with DuckDB."""
        # Replace NaN with None
        df = df.replace({np.nan: None})
        
        # Convert integer columns to Int64 (nullable integer)
        for col in df.select_dtypes(include=['int']).columns:
            df[col] = df[col].astype('Int64')
            
        # Convert float columns to float64
        for col in df.select_dtypes(include=['float']).columns:
            df[col] = df[col].astype('float64')

        return df
    
    def extract_tables(self, plan: Dict[str, List[Dict[str, Any]]]) -> None:
        """
        Extract tables from Snowflake according to the plan.
        
        Args:
            plan: Dictionary mapping database.schema to list of tables to extract
            
        Returns:
            Dictionary mapping table names to extracted DataFrames
        """
        results = {}
        schema_stats = {}  # Track stats by schema
        
        # Calculate total tables across all schemas
        total_tables = sum(len(tables) for tables in plan.values())
        print_info(f"\nStarting extraction of {total_tables} tables...")

        # Get connection
        conn = self._get_connection()
        
        # Create DuckDB connection
        db_dir = Path(self.config.duckdb.database_path).parent
        os.makedirs(db_dir, exist_ok=True)
        duckdb_conn = duckdb.connect(str(self.config.duckdb.database_path))
        
        try:
            # Process each database.schema
            for db_schema, tables in plan.items():
                database, schema = db_schema.split('.')
                
                # Initialize schema stats
                schema_key = f"{database}.{schema}"
                schema_stats[schema_key] = {
                    'tables': 0,
                    'rows': 0
                }
                
                # Attach database and create schema in DuckDB if they don't exist
                duckdb_conn.execute(f"ATTACH IF NOT EXISTS DATABASE '{db_dir}/{database}.duckdb' AS {database}")
                duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
                
                # Extract each table
                for i, table in enumerate(tables, 1):
                    table_name = table['table_name']
                    full_table_name = f"{database}.{schema}.{table_name}"
                    
                    with print_status(f"Extracting [{i}/{total_tables}] {full_table_name}..."):
                        # Build query with row limit
                        query = f"""
                        SELECT *
                        FROM identifier('{full_table_name}')
                        LIMIT {self.config.extract.row_limit}
                        """
                            
                        # Execute query and fetch results
                        cursor = conn.cursor()
                        try:
                            cursor.execute(f'USE WAREHOUSE {self.config.warehouse.warehouse}')
                            cursor.execute(query)
                        except Exception as e:
                            print_error(f"{full_table_name}: Failed to extract - {str(e)}")
                            continue
                        
                        # Fetch in batches
                        while True:
                            df = cursor.fetch_pandas_all()
                            if df is None or len(df) == 0:
                                break
                                
                            # Convert types for DuckDB compatibility
                            df = self._convert_df_for_duckdb(df)
                            
                            try:
                                # Try writing directly to DuckDB
                                duckdb_conn.execute(f"""
                                    CREATE OR REPLACE TABLE {full_table_name} AS 
                                    SELECT * FROM table_name
                                """)
                                print_success(f"{full_table_name}: {len(df):,} rows")
                            except Exception as e:
                                # If direct write fails, try using parquet as intermediate
                                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                                    df.to_parquet(tmp.name)
                                    try:
                                        duckdb_conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")
                                        duckdb_conn.execute(f"""
                                            CREATE TABLE {full_table_name} AS 
                                            SELECT * FROM parquet_scan('{tmp.name}')
                                        """)
                                        print_success(f"{full_table_name}: {len(df):,} rows")
                                    finally:
                                        os.unlink(tmp.name)
                            
                            # Update schema stats
                            schema_stats[schema_key]['tables'] += 1
                            schema_stats[schema_key]['rows'] += len(df)

                            # Only process one batch if row_limit is hit
                            if self.config.extract.row_limit:
                                break
                                
                        cursor.close()
                        
        finally:
            duckdb_conn.close()
            
        # Print schema summary
        print_info("\nExtraction Summary:")
        for schema, stats in schema_stats.items():
            print_info(f"  • {schema}: {stats['tables']} tables, {stats['rows']:,} rows")
            
        return
