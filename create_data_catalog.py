from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkClientException, SnowparkSQLException
import pandas as pd
import json


conn_params = {
    'user': 'kzhang',
    'password': 'rfk.bnt3vjx9MUR4qzt',
    'account': 'pljbcel-gw79467',
    'warehouse': 'COMPUTE_WH',
    'database': 'BCP',
    'schema': 'INFORMATION_SCHEMA'
}

def create_session(configs):
    try:
        session = Session.builder.configs(configs['SNOWFLAKE']).create()
        print('Snowflake session created successfully')
        return session
    except (SnowparkClientException, SnowparkSQLException) as e:
        print(f'Error creating Snowflake session: {e}')
        raise

session = Session.builder.configs(conn_params).create()

queries = {
    'schemas': """
        SELECT
            schema_name,
            schema_owner,
            created,
            last_altered
        FROM
            INFORMATION_SCHEMA.SCHEMATA;
    """,
    'tables': """
        SELECT
            table_schema,
            table_name,
            clustering_key,
            row_count,
            created,
            last_altered
        FROM
            INFORMATION_SCHEMA.TABLES;
    """,
    'columns': """
        SELECT
            table_schema,
            table_name,
            column_name,
            ordinal_position,
            column_default,
            is_nullable,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM
            INFORMATION_SCHEMA.COLUMNS
        ORDER BY
            table_name, ordinal_position;
    """,
    'constraints': """
        SELECT
            table_schema,
            table_name,
            constraint_name,
            constraint_type
        FROM
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS;
    """
}

# Function to execute a query and return a DataFrame
def execute_query(query):
    return session.sql(query).to_pandas()

# Execute queries and store results in DataFrames
schemas_df = execute_query(queries['schemas'])
tables_df = execute_query(queries['tables'])
columns_df = execute_query(queries['columns'])
constraints_df = execute_query(queries['constraints'])

# Save DataFrames to CSV files
schemas_df.to_csv('schemas_metadata.csv', index=False)
tables_df.to_csv('tables_metadata.csv', index=False)
columns_df.to_csv('columns_metadata.csv', index=False)
constraints_df.to_csv('constraints_metadata.csv', index=False)

# Close the session
session.close()
