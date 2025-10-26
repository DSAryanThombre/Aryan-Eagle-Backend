import os
import json
from datetime import datetime
import pandas as pd
import warnings
import uuid
from django.db import connections # Import Django's connection handler
from django.conf import settings
warnings.filterwarnings('ignore')
import logging

logger = logging.getLogger(__name__)

# --- We remove the manual connection helpers and use Django's built-in connections ---
# def _get_snowflake_connection(...): ...
# def get_db_connection(...): ...

# --- Helper to execute a query and return results as a Pandas DataFrame ---
def execute_query_to_dataframe(connection_name: str, query: str, params: tuple = None):
    """
    Executes a query and returns results as a Pandas DataFrame using Django's connection.
    Supports parameterized queries to prevent SQL Injection.
    """
    try:
        if not query.upper().strip().startswith("SELECT"):
            return pd.DataFrame(), "Only SELECT queries are allowed for data retrieval."
        
        # Use Django's connections object to get a live connection
        with connections[connection_name].cursor() as cursor:
            # Use pandas.read_sql, which handles parameterization
            df = pd.read_sql(query, connections[connection_name], params=params)

        if df.empty:
            return df, None
        
        return df, None
    except Exception as e:
        logger.error(f"Error executing query to DataFrame for connection '{connection_name}': {e}")
        return pd.DataFrame(), f"SQL execution error: {e}"

# --- The dynamic query builder remains largely unchanged ---
def build_dynamic_aggregation_query(db_config, table, date_column, date_value,
                                    aggregation_type, aggregation_column,
                                    group_by_column=None, additional_filters=None):
    """
    Builds a dynamic SQL aggregation query based on provided parameters.
    """
    # This logic is solid and remains the same.
    database = db_config.get('db') or db_config.get('NAME')
    schema = db_config.get('schema')
    if not database or not schema:
        raise ValueError("Database config must include both 'db'/'NAME' and 'schema'.")

    # Always use Snowflake table reference
    table_ref = f"{database}.{schema}.{table}"

    agg_type_upper = aggregation_type.strip().upper()

    if agg_type_upper == 'COUNT(*)':
        agg_clause = 'COUNT(*)'
    elif agg_type_upper in ['COUNT','SUM', 'AVG', 'MIN', 'MAX'] and aggregation_column:
        agg_clause = f"{agg_type_upper}({aggregation_column})"
    else:
        raise ValueError(f"Invalid aggregation_type '{aggregation_type}' or missing aggregation_column for type '{aggregation_type}'.")

    # Always use Snowflake date filter logic
    if date_value:
        if date_value.strip().upper() == 'CURRENT_DATE()':
            date_filter = f"TO_DATE({date_column}) = CURRENT_DATE()"
        else:
            date_filter = f"TO_DATE({date_column}) = TO_DATE('{date_value}', 'YYYY-MM-DD')"
    else:
        date_filter = ""

    group_by_clause = ""
    if group_by_column:
        group_by_cols = [col.strip() for col in group_by_column.split(',') if col.strip()]
        if group_by_cols:
            group_by_clause = f"GROUP BY {', '.join(group_by_cols)}"

    where_clauses = []
    if date_filter:
        where_clauses.append(date_filter)
    if additional_filters:
        where_clauses.append(f"({additional_filters})")

    full_where_clause = ""
    if where_clauses:
        full_where_clause = "WHERE " + " AND ".join(where_clauses)

    query = (
        f"SELECT {agg_clause} "
        f"{', ' + group_by_column if group_by_column else ''} "
        f"FROM {table_ref} {full_where_clause} "
        f"{group_by_clause};"
    )

    return query

def build_dynamic_dax_query_powerbi(table, date_column=None, date_value=None,
                                   aggregation_type="COUNT", aggregation_column=None,
                                   group_by_column=None, additional_filters=None, custom_dax_query=None):
    """
    Builds a dynamic DAX query string for Power BI based on user input.
    If custom_dax_query is provided, returns it directly.
    If no group_by_column is provided, returns a scalar value using CALCULATE/ROW.
    """
    if custom_dax_query and custom_dax_query.strip():
        return custom_dax_query.strip()

    # Quote table name if it contains spaces
    table_quoted = f"'{table}'" if ' ' in table else table

    # Quote column names if they contain spaces
    def quote_column(col):
        return f"'{col}'" if col and ' ' in col else col

    date_column_quoted = quote_column(date_column)
    aggregation_column_quoted = quote_column(aggregation_column)

    agg_type_upper = aggregation_type.strip().upper()
    group_by_cols = [col.strip() for col in (group_by_column or '').split(',') if col.strip()]
    group_by_clause = ", ".join([f'"{col}"' for col in group_by_cols]) if group_by_cols else ""

    # Date filter for DAX
    date_filter = ""
    if date_column and date_value:
        if str(date_value).strip().upper() == "CURRENT_DATE()":
            date_filter = f'{table_quoted}[{date_column_quoted}] = TODAY()'
        else:
            date_filter = f'{table_quoted}[{date_column_quoted}] = DATEVALUE("{date_value}")'

    # Additional filters for DAX
    filters = []
    if date_filter:
        filters.append(date_filter)
    if additional_filters:
        filters.append(additional_filters)
    filter_str = " && ".join(filters) if filters else ""

    # Aggregation clause for DAX
    if agg_type_upper == "COUNT":
        agg_clause = f'COUNTROWS({table_quoted})'
    elif agg_type_upper in ["SUM", "AVG", "MIN", "MAX"] and aggregation_column:
        agg_clause = f'{agg_type_upper}({table_quoted}[{aggregation_column_quoted}])'
    else:
        agg_clause = f'COUNTROWS({table_quoted})'

    # Build DAX query
    if group_by_clause:
        # Use SUMMARIZECOLUMNS for group by
        if filter_str:
            dax_query = f"""
                EVALUATE
                SUMMARIZECOLUMNS(
                    {group_by_clause},
                    FILTER({table_quoted}, {filter_str}),
                    "Value", {agg_clause}
                )
            """
        else:
            dax_query = f"""
                EVALUATE
                SUMMARIZECOLUMNS(
                    {group_by_clause},
                    "Value", {agg_clause}
                )
            """
    else:
        # Always use CALCULATE/ROW for scalar value
        if filter_str:
            dax_query = f"""
                EVALUATE
                VAR __value = CALCULATE({agg_clause}, {filter_str})
                RETURN
                ROW("Value", __value)
            """
        else:
            dax_query = f"""
                EVALUATE
                VAR __value = {agg_clause}
                RETURN
                ROW("Value", __value)
            """
    return dax_query.strip()
