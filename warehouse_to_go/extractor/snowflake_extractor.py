from typing import Dict, List, Optional, Any
import snowflake.connector
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
import duckdb
import os
import numpy as np
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from warehouse_to_go.utils.config import Config
from warehouse_to_go.utils.output import print_info, print_success, print_status, print_error


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
            
        # Convert inexact number columns to float64
        for col in df.select_dtypes(include=['inexact']).columns:
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

        # Get warehouse connection
        conn = self._get_connection()

        # Create DuckDB connection
        db_dir = Path(self.config.duckdb.database_path).parent
        os.makedirs(db_dir, exist_ok=True)
        duckdb_conn = duckdb.connect(str(self.config.duckdb.database_path))
        """this used to deal with out of range decimals
        TODO: figure more elegant way to sample over batches
        """
        duckdb_conn.execute(f"SET GLOBAL pandas_analyze_sample = 10000")
        
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
                duckdb_conn.execute(f"ATTACH IF NOT EXISTS DATABASE '{Path(db_dir) / database}.duckdb' AS {database}")
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

                        # Warehouse caps rows at row_limit. Chunk into
                        # ceil(row_limit / batch_size) batches; the final batch
                        # holds the remainder from the division. 'df' is
                        # auto-registered by DuckDB, so batch 0 seeds the table
                        # and later batches append to it.
                        row_limit = self.config.extract.row_limit
                        batch_size = self.config.extract.batch_size
                        num_batches = (row_limit + batch_size - 1) // batch_size
                        fetched = 0
                        for batch_number in range(num_batches):
                            rows = cursor.fetchmany(size=batch_size)
                            if not rows:
                                break
                            df = pd.DataFrame(rows, columns=[d[0] for d in cursor.description])
                            df = self._convert_df_for_duckdb(df)
                            if batch_number == 0:
                                # create the table structure for type inference issues on the full dataset.
                                duckdb_conn.execute(f"CREATE OR REPLACE TABLE {full_table_name} AS SELECT * FROM df LIMIT 0")
                            duckdb_conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM df")
                            fetched += len(df)

                        print_success(f"{full_table_name}: {fetched:,} rows")

                        # Update schema stats
                        schema_stats[schema_key]['tables'] += 1
                        schema_stats[schema_key]['rows'] += fetched
                                
                        cursor.close()
                        
        finally:
            duckdb_conn.close()
            
        # Print schema summary
        print_info("\nExtraction Summary:")
        for schema, stats in schema_stats.items():
            print_info(f"  • {schema}: {stats['tables']} tables, {stats['rows']:,} rows")
            
        return
