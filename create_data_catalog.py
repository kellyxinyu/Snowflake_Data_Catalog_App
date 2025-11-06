from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkClientException, SnowparkSQLException
import pandas as pd
import json
import logging


def load_config(config_file):
    with open(config_file, 'r') as file:
        return json.load(file)

def create_session(configs):
    private_key_path = configs.get("private_key_path")
    if private_key_path:
        # Read the private key file
        with open(private_key_path, "rb") as key_file:
            private_key = key_file.read()
        configs["private_key"] = private_key
        # Remove private_key_path from configs to avoid conflicts
        configs.pop("private_key_path", None)
    # Ensure no password is included for key-pair authentication
    configs.pop("password", None)

    try:
        session = Session.builder.configs(configs).create()
        logging.info('Snowflake session created successfully')
        return session
    except (SnowparkClientException, SnowparkSQLException) as e:
        logging.error(f'Error creating Snowflake session: {e}')
        raise

config_path = './config.json'
config = load_config(config_path)
session_params = config['SNOWFLAKE']
session = create_session(session_params)


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
