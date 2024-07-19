# import streamlit as st
# import pandas as pd

# # Load the metadata from CSV files
# tables_df = pd.read_csv('tables_metadata.csv')
# columns_df = pd.read_csv('columns_metadata.csv')
# constraints_df = pd.read_csv('constraints_metadata.csv')

# # Streamlit app
# st.title("Snowflake Data Catalog")

# st.header("Tables")
# st.dataframe(tables_df)

# st.header("Columns")
# st.dataframe(columns_df)

# st.header("Constraints")
# st.dataframe(constraints_df)

import streamlit as st
import pandas as pd

# Load the metadata from CSV files
schemas_df = pd.read_csv('schemas_metadata.csv')
tables_df = pd.read_csv('tables_metadata.csv')
columns_df = pd.read_csv('columns_metadata.csv')
constraints_df = pd.read_csv('constraints_metadata.csv')

# Streamlit app
st.title("Snowflake Data Catalog")

# Select box to choose a schema
select_schema = st.selectbox("Select a schema to view details:", schemas_df['SCHEMA_NAME'].unique())

if select_schema:
    # Filter tables based on selected schema
    filtered_tables_df = tables_df[tables_df['TABLE_SCHEMA'] == select_schema]

    # Select box to choose a table
    selected_table = st.selectbox("Select a table to view details:", filtered_tables_df['TABLE_NAME'].unique())

    if selected_table:
        # st.header(f"Details for Table: {selected_table} in Schema: {select_schema}")

        # Display table metadata
        table_metadata = filtered_tables_df[filtered_tables_df['TABLE_NAME'] == selected_table]
        st.subheader("Table Metadata")
        st.dataframe(table_metadata)

        # Display columns metadata
        columns_metadata = columns_df[(columns_df['TABLE_NAME'] == selected_table) & (columns_df['TABLE_SCHEMA'] == select_schema)]
        st.subheader("Columns Metadata")
        st.dataframe(columns_metadata)

        # Display constraints metadata
        constraints_metadata = constraints_df[(constraints_df['TABLE_NAME'] == selected_table) & (constraints_df['TABLE_SCHEMA'] == select_schema)]
        st.subheader("Constraints Metadata")
        st.dataframe(constraints_metadata)
