# dq_management/test_case_manager.py

import uuid
from datetime import datetime, timedelta
import pandas as pd
import logging
import copy
from django.conf import settings
from dq_management.dq_core import (
    execute_query_to_dataframe,
    build_dynamic_aggregation_query,
    build_dynamic_dax_query_powerbi
)
from dq_management.powerbi_connector import PowerBIConnector

logger = logging.getLogger(__name__)

class TestCaseProcessor:
    def __init__(self):
        pass

    def _perform_comparison(self, source_val, destination_val, threshold, threshold_type):
        overall_status = "ERROR"
        overall_message = "Comparison logic error."
        test_outcome = "ERROR"
        difference = None
        if source_val is None or destination_val is None:
            overall_status = "FAIL"
            overall_message = "One or both queries returned no comparable result (value is None)."
            test_outcome = "FAIL"
            return overall_status, overall_message, test_outcome, difference
        try:
            s_val_num = float(source_val)
            d_val_num = float(destination_val)
            abs_difference = abs(s_val_num - d_val_num)
            is_pass = False
            outcome_msg = ""
            if threshold_type == "ABSOLUTE":
                difference = abs_difference
                is_pass = difference <= threshold
                outcome_msg = f"Absolute difference: {difference:.4f}. Threshold: {threshold:.4f}."
            elif threshold_type == "PERCENTAGE":
                if s_val_num == 0:
                    percentage_diff = 0.0 if d_val_num == 0 else float('inf')
                    difference = percentage_diff
                    is_pass = (d_val_num == 0)
                    outcome_msg = f"Source value is 0. Destination value: {d_val_num}. Percentage difference: {'Infinity' if d_val_num != 0 else '0'}%."
                else:
                    percentage_diff = (abs_difference / abs(s_val_num)) * 100
                    difference = percentage_diff
                    is_pass = percentage_diff <= threshold
                    outcome_msg = f"Percentage difference: {percentage_diff:.2f}%. Threshold: {threshold:.2f}%."
            else:
                overall_status = "ERROR"
                test_outcome = "ERROR"
                overall_message = "Invalid threshold type configured."
                return overall_status, overall_message, test_outcome, difference
            if is_pass:
                overall_status = "PASS"
                test_outcome = "PASS"
                overall_message = f"Test Passed. {outcome_msg}"
            else:
                overall_status = "FAIL"
                test_outcome = "FAIL"
                overall_message = f"Test Failed. {outcome_msg} Source: {s_val_num}, Dest: {d_val_num}."
        except ValueError:
            if str(source_val).strip() == str(destination_val).strip():
                overall_status = "PASS"
                test_outcome = "PASS"
                overall_message = "Values are identical (non-numeric comparison)."
                difference = 0
            else:
                overall_status = "FAIL"
                test_outcome = "FAIL"
                overall_message = f"Non-numeric values mismatch. Source: '{source_val}', Destination: '{destination_val}'."
                difference = "N/A"
        except Exception as e:
            overall_status = "ERROR"
            test_outcome = "ERROR"
            overall_message = f"An unexpected error occurred during comparison: {e}"
            difference = "N/A"
        return overall_status, overall_message, test_outcome, difference
    
    def _map_user_connection(self, user_conn_name):
        mapping = {
            'PROD': 'snowflake_prod',
            'DEV': 'snowflake_dev',
        }
        # If Power BI is selected, return as-is (do not map to a Django DB key)
        if user_conn_name == "Power BI":
            return "Power BI"
        return mapping.get(user_conn_name, user_conn_name)

    def process_test_request(self, config, action, run_id=None):
        working_config = copy.deepcopy(config)
        test_type = working_config.get('test_type').lower().replace(' ', '')
        
        result_data = {
            "status": "ERROR", "message": "An unhandled error occurred during test processing.",
            "source_value": None, "destination_value": None, "difference": None,
            "threshold": working_config.get('threshold'), "threshold_type": working_config.get('threshold_type'),
            "source_query": "N/A", "destination_query": "N/A", "test_outcome": "ERROR",
            "source_connection_used": "N/A", "destination_connection_used": "N/A",
        }
        try:
            if test_type == 'aggregationcomparison':
                result_data.update(self._execute_snowflake_to_powerbi_aggregation_test_logic(working_config))
            elif test_type == 'drifttest' or test_type == 'powerbidrift':
                result_data.update(self._execute_drift_test_logic(working_config))
            elif test_type == 'availabilitytest' or test_type == 'powerbiavailability':
                result_data.update(self._execute_availability_test_logic(working_config))
            else:
                raise ValueError(f"Unknown or unsupported test type: {test_type}")
        except Exception as e:
            logger.exception(f"Unhandled error in process_test_request for test {config.get('test_name', 'Unnamed')}:")
            result_data['message'] = f"An unhandled error occurred: {e}"
        return result_data

    # --- Aggregation Test: Snowflake Source, Power BI Destination ---
    def _execute_snowflake_to_powerbi_aggregation_test_logic(self, config):
        result = {}

        # --- Source Side ---
        source_conn_name = self._map_user_connection(config.get('source_connection_source'))
        if source_conn_name == "Power BI":
            if config.get('source_dax_query', '').strip():
                dax_query = config['source_dax_query'].strip()
            else:
                dax_query = build_dynamic_dax_query_powerbi(
                    table=config.get('source_table'), date_column=config.get('source_date_column'),
                    date_value=config.get('source_date_value'), aggregation_type=config.get('source_aggregation_type', 'COUNT'),
                    aggregation_column=config.get('source_aggregation_column'), group_by_column=config.get('source_group_by_column'),
                    additional_filters=config.get('source_additional_filters')
                )
            result['source_query'] = dax_query
            try:
                df_source = PowerBIConnector()._execute_dax_query(
                    config.get('source_workspace_id'),  # <-- use correct key
                    config.get('source_dataset_id'),    # <-- use correct key
                    dax_query
                )
                result['source_value'] = df_source.iloc[0, 0] if not df_source.empty and not df_source.columns.empty else 0
                result['source_connection_used'] = "Power BI"
            except Exception as e:
                result['source_value'] = None
                result['source_connection_used'] = "Power BI"
                result['source_error'] = str(e)
        else:
            # Use SQL builder and Snowflake executor for source
            source_db_config = dict(settings.DATABASES[source_conn_name])
            source_db_config['schema'] = config.get('source_schema') or source_db_config.get('schema') or 'PUBLIC'
            source_query_str = build_dynamic_aggregation_query(
                source_db_config, config.get('source_table'), config.get('source_date_column'),
                config.get('source_date_value'), config.get('source_aggregation_type'),
                config.get('source_aggregation_column'), config.get('source_group_by_column'),
                config.get('source_additional_filters')
            )
            result['source_query'] = source_query_str
            df_source, source_exec_error = execute_query_to_dataframe(source_conn_name, source_query_str)
            if source_exec_error:
                result['source_value'] = None
                result['source_connection_used'] = source_conn_name
                result['source_error'] = f"Source query failed: {source_exec_error}"
            else:
                result['source_value'] = df_source.iloc[0, 0] if not df_source.empty and not df_source.columns.empty else 0
                result['source_connection_used'] = source_conn_name

        # --- Destination Side ---
        destination_conn_name = self._map_user_connection(config.get('destination_connection_source'))
        if destination_conn_name == "Power BI":
            dax_query = config.get('destination_dax_query', '').strip()
            if not dax_query:
                dax_query = build_dynamic_dax_query_powerbi(
                    table=config.get('destination_table'), date_column=config.get('destination_date_column'),
                    date_value=config.get('destination_date_value'), aggregation_type=config.get('destination_aggregation_type', 'COUNT'),
                    aggregation_column=config.get('destination_aggregation_column'), group_by_column=config.get('destination_group_by_column'),
                    additional_filters=config.get('destination_additional_filters')
                )
            result['destination_query'] = dax_query
            try:
                df_destination = PowerBIConnector()._execute_dax_query(
                    config.get('destination_workspace_id'),  # <-- use correct key
                    config.get('destination_dataset_id'),    # <-- use correct key
                    dax_query
                )
                result['destination_value'] = df_destination.iloc[0, 0] if not df_destination.empty and not df_destination.columns.empty else 0
                result['destination_connection_used'] = "Power BI"
            except Exception as e:
                result['destination_value'] = None
                result['destination_connection_used'] = "Power BI"
                result['destination_error'] = str(e)
        else:
            # Use SQL builder and Snowflake executor for destination
            destination_db_config = dict(settings.DATABASES[destination_conn_name])
            destination_db_config['schema'] = config.get('destination_schema') or destination_db_config.get('schema') or 'PUBLIC'
            destination_query_str = build_dynamic_aggregation_query(
                destination_db_config, config.get('destination_table'), config.get('destination_date_column'),
                config.get('destination_date_value'), config.get('destination_aggregation_type'),
                config.get('destination_aggregation_column'), config.get('destination_group_by_column'),
                config.get('destination_additional_filters')
            )
            result['destination_query'] = destination_query_str
            df_destination, destination_exec_error = execute_query_to_dataframe(destination_conn_name, destination_query_str)
            if destination_exec_error:
                result['destination_value'] = None
                result['destination_connection_used'] = destination_conn_name
                result['destination_error'] = f"Destination query failed: {destination_exec_error}"
            else:
                result['destination_value'] = df_destination.iloc[0, 0] if not df_destination.empty and not df_destination.columns.empty else 0
                result['destination_connection_used'] = destination_conn_name

        # --- Comparison ---
        if result.get('source_value') is not None and result.get('destination_value') is not None:
            status, message, outcome, diff = self._perform_comparison(
                result['source_value'], result['destination_value'], config.get('threshold'), config.get('threshold_type')
            )
            result.update({"status": status, "message": message, "difference": diff, "test_outcome": outcome})
        else:
            result['status'] = "ERROR"
            result['message'] = (
                f"Source error: {result.get('source_error', '')} "
                f"Destination error: {result.get('destination_error', '')}"
            ).strip()
            result['test_outcome'] = "ERROR"
            result['difference'] = None
        return result

    # --- Drift Test (Single-system) ---
    def _execute_drift_test_logic(self, config):
        result = {}
        data_source = config.get('source_connection_source')
        today_date_value = 'CURRENT_DATE()'
        yesterday_date_value = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        if data_source == 'Power BI':
            dax_query_today = build_dynamic_dax_query_powerbi(
                table=config.get('source_table'), date_column=config.get('source_date_column'), date_value=today_date_value,
                aggregation_type=config.get('source_aggregation_type', 'COUNT'), aggregation_column=config.get('source_aggregation_column')
            )
            df_today = PowerBIConnector()._execute_dax_query(
                config.get('source_workspace_id'),  # <-- use correct key
                config.get('source_dataset_id'),    # <-- use correct key
                dax_query_today
            )
            dax_query_yesterday = build_dynamic_dax_query_powerbi(
                table=config.get('source_table'), date_column=config.get('source_date_column'), date_value=yesterday_date_value,
                aggregation_type=config.get('source_aggregation_type', 'COUNT'), aggregation_column=config.get('source_aggregation_column')
            )
            df_yesterday = PowerBIConnector()._execute_dax_query(
                config.get('source_workspace_id'),  # <-- use correct key
                config.get('source_dataset_id'),    # <-- use correct key
                dax_query_yesterday
            )
            result['source_value'] = df_today.iloc[0, 0] if not df_today.empty and not df_today.columns.empty else 0
            result['destination_value'] = df_yesterday.iloc[0, 0] if not df_yesterday.empty and not df_yesterday.columns.empty else 0
            result['source_query'] = dax_query_today
            result['destination_query'] = dax_query_yesterday
            result['source_connection_used'] = 'Power BI'
            result['destination_connection_used'] = 'Power BI'
        else:
            conn_name = self._map_user_connection(data_source)
            db_config = dict(settings.DATABASES[conn_name])
            db_config['schema'] = config.get('source_schema') or db_config.get('schema') or 'PUBLIC'
            sql_query_today = build_dynamic_aggregation_query(db_config, config.get('source_table'), config.get('source_date_column'), today_date_value, config.get('source_aggregation_type'), config.get('source_aggregation_column'))
            df_today, exec_error_today = execute_query_to_dataframe(conn_name, sql_query_today)
            if exec_error_today: raise Exception(f"Today's query failed: {exec_error_today}")
            sql_query_yesterday = build_dynamic_aggregation_query(db_config, config.get('source_table'), config.get('source_date_column'), yesterday_date_value, config.get('source_aggregation_type'), config.get('source_aggregation_column'))
            df_yesterday, exec_error_yesterday = execute_query_to_dataframe(conn_name, sql_query_yesterday)
            if exec_error_yesterday: raise Exception(f"Yesterday's query failed: {exec_error_yesterday}")
            result['source_value'] = df_today.iloc[0, 0] if not df_today.empty and not df_today.columns.empty else 0
            result['destination_value'] = df_yesterday.iloc[0, 0] if not df_yesterday.empty and not df_yesterday.columns.empty else 0
            result['source_query'] = sql_query_today
            result['destination_query'] = sql_query_yesterday
            result['source_connection_used'] = conn_name
            result['destination_connection_used'] = conn_name
        status, message, outcome, diff = self._perform_comparison(
            result['source_value'], result['destination_value'], config.get('threshold', 0), config.get('threshold_type')
        )
        result.update({"status": status, "message": message, "difference": diff, "test_outcome": outcome})
        result.update({'threshold': config.get('threshold'), 'threshold_type': config.get('threshold_type')})
        return result

    # --- Availability Test (Single-system) ---
    def _execute_availability_test_logic(self, config):
        result = {}
        # --- Source Side ---
        data_source = config.get('source_connection_source')
        if data_source == 'Power BI':
            dax_query = build_dynamic_dax_query_powerbi(
                table=config.get('source_table'), date_column=config.get('source_date_column'), date_value='CURRENT_DATE()',
                aggregation_type='COUNT', aggregation_column=config.get('source_aggregation_column')
            )
            df_source = PowerBIConnector()._execute_dax_query(
                config.get('source_workspace_id'),  # <-- use correct key
                config.get('source_dataset_id'),    # <-- use correct key
                dax_query
            )
            result['source_value'] = df_source.iloc[0, 0] if not df_source.empty and not df_source.columns.empty else 0
            result['source_query'] = dax_query
            result['source_connection_used'] = 'Power BI'
        else:
            conn_name = self._map_user_connection(data_source)
            db_config = dict(settings.DATABASES[conn_name])
            db_config['schema'] = config.get('source_schema') or db_config.get('schema') or 'PUBLIC'
            sql_query = build_dynamic_aggregation_query(db_config, config.get('source_table'), config.get('source_date_column'), 'CURRENT_DATE()', 'COUNT', config.get('source_aggregation_column'))
            df_source, exec_error = execute_query_to_dataframe(conn_name, sql_query)
            if exec_error: raise Exception(f"Availability query failed: {exec_error}")
            result['source_value'] = df_source.iloc[0, 0] if not df_source.empty and not df_source.columns.empty else 0
            result['source_query'] = sql_query
            result['source_connection_used'] = conn_name

        # --- Destination Side (only if configured) ---
        data_dest = config.get('destination_connection_source')
        destination_configured = bool(data_dest)

        if destination_configured:
            if data_dest == 'Power BI':
                dax_query_dest = build_dynamic_dax_query_powerbi(
                    table=config.get('destination_table'), date_column=config.get('destination_date_column'), date_value='CURRENT_DATE()',
                    aggregation_type='COUNT', aggregation_column=config.get('destination_aggregation_column'),
                    custom_dax_query=config.get('destination_dax_query', '')
                )
                df_dest = PowerBIConnector()._execute_dax_query(
                    config.get('destination_workspace_id'),
                    config.get('destination_dataset_id'),
                    dax_query_dest
                )
                result['destination_value'] = df_dest.iloc[0, 0] if not df_dest.empty and not df_dest.columns.empty else 0
                result['destination_query'] = dax_query_dest
                result['destination_connection_used'] = 'Power BI'
            else:
                conn_name_dest = self._map_user_connection(data_dest)
                db_config_dest = dict(settings.DATABASES[conn_name_dest])
                db_config_dest['schema'] = config.get('destination_schema') or db_config_dest.get('schema') or 'PUBLIC'
                sql_query_dest = build_dynamic_aggregation_query(db_config_dest, config.get('destination_table'), config.get('destination_date_column'), 'CURRENT_DATE()', 'COUNT', config.get('destination_aggregation_column'))
                df_dest, exec_error_dest = execute_query_to_dataframe(conn_name_dest, sql_query_dest)
                if exec_error_dest: raise Exception(f"Availability query failed: {exec_error_dest}")
                result['destination_value'] = df_dest.iloc[0, 0] if not df_dest.empty and not df_dest.columns.empty else 0
                result['destination_query'] = sql_query_dest
                result['destination_connection_used'] = conn_name_dest
        else:
            result['destination_value'] = None
            result['destination_query'] = "N/A"
            result['destination_connection_used'] = "N/A"


        # --- Comparison: Logic depends on whether destination is configured ---
        if destination_configured:
            # PASS only if both source and destination have data (>0)
            source_val = result.get('source_value', 0) or 0
            dest_val = result.get('destination_value', 0) or 0
            if source_val > 0 and dest_val > 0:
                status, message, outcome, diff = ('PASS', 'Data is available in both source and destination.', 'PASS', 0)
            else:
                status, message, outcome, diff = ('FAIL', 'Data is not available in one or both systems.', 'FAIL', 'N/A')
        else:
            # PASS if source has data (>0)
            source_val = result.get('source_value', 0) or 0
            if source_val > 0:
                status, message, outcome, diff = ('PASS', 'Data is available in the source.', 'PASS', 0)
            else:
                status, message, outcome, diff = ('FAIL', 'Data is not available in the source.', 'FAIL', 'N/A')

        result.update({"status": status, "message": message, "difference": diff, "test_outcome": outcome})
        return result

    # The following methods are only used by the new main logic functions.
    def _execute_aggregation_test(self, config):
        source_conn_name = self._map_user_connection(config.get('source_connection_source'))
        destination_conn_name = self._map_user_connection(config.get('destination_connection_source'))
        source_db_config = dict(settings.DATABASES[source_conn_name])
        source_db_config['schema'] = config.get('source_schema') or source_db_config.get('schema') or 'PUBLIC'
        destination_db_config = dict(settings.DATABASES[destination_conn_name])
        destination_db_config['schema'] = config.get('destination_schema') or destination_db_config.get('schema') or 'PUBLIC'
        source_is_custom_sql = (config.get('source_custom_sql') or '').strip() != ''
        if source_is_custom_sql:
            source_query_str = config['source_custom_sql']
        else:
            source_query_str = build_dynamic_aggregation_query(
                source_db_config, config.get('source_table'), config.get('source_date_column'),
                config.get('source_date_value'), config.get('source_aggregation_type'),
                config.get('source_aggregation_column'), config.get('source_group_by_column'),
                config.get('source_additional_filters')
            )
        dest_is_custom_sql = (config.get('destination_custom_sql') or '').strip() != ''
        if dest_is_custom_sql:
            destination_query_str = config['destination_custom_sql']
        else:
            destination_query_str = build_dynamic_aggregation_query(
                destination_db_config, config.get('destination_table'), config.get('destination_date_column'),
                config.get('destination_date_value'), config.get('destination_aggregation_type'),
                config.get('destination_aggregation_column'), config.get('destination_group_by_column'),
                config.get('destination_additional_filters')
            )
        return source_query_str, destination_query_str, source_conn_name, destination_conn_name, config
    
    def _build_drift_query(self, config):
        table, date_column, agg_type, agg_column = config.get('source_table'), config.get('source_date_column'), config.get('source_aggregation_type'), config.get('source_aggregation_column')
        group_by_column, additional_filters = config.get('source_group_by_column'), config.get('source_additional_filters')
        db_config = dict(settings.DATABASES[config['source_connection_source']])
        db_config['schema'] = config.get('source_schema') or db_config.get('schema') or 'PUBLIC'
        today_query = build_dynamic_aggregation_query(db_config, table, date_column, 'CURRENT_DATE()', agg_type, agg_column, group_by_column, additional_filters)
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        yesterday_query = build_dynamic_aggregation_query(db_config, table, date_column, yesterday_str, agg_type, agg_column, group_by_column, additional_filters)
        return today_query, yesterday_query

    def _execute_drift_test(self, config):
        modified_config = copy.deepcopy(config)
        source_query_str, destination_query_str = self._build_drift_query(modified_config)
        source_conn_name = modified_config.get('source_connection_source')
        destination_conn_name = source_conn_name
        modified_config.update({
            'destination_connection_source': destination_conn_name, 'destination_table': modified_config['source_table'],
            'destination_date_column': modified_config['source_date_column'], 'destination_aggregation_type': modified_config['source_aggregation_type'],
            'destination_aggregation_column': modified_config['source_aggregation_column'], 'destination_additional_filters': modified_config['source_additional_filters'],
            'destination_custom_sql': modified_config['source_custom_sql'], 'source_date_value': 'CURRENT_DATE()',
            'destination_date_value': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'), 'threshold_type': 'PERCENTAGE'
        })
        return source_query_str, destination_query_str, source_conn_name, destination_conn_name, modified_config

    def _execute_availability_test(self, config):
        modified_config = copy.deepcopy(config)
        source_agg_column = modified_config.get('source_aggregation_column')
        destination_agg_column = modified_config.get('destination_aggregation_column')
        modified_config.update({
            'source_aggregation_type': 'COUNT', 'source_date_value': 'CURRENT_DATE()',
            'destination_aggregation_type': 'COUNT', 'destination_date_value': 'CURRENT_DATE()'
        })
        source_conn_name = self._map_user_connection(modified_config.get('source_connection_source'))
        destination_conn_name = self._map_user_connection(modified_config.get('destination_connection_source'))
        source_db_config = dict(settings.DATABASES[source_conn_name])
        source_db_config['schema'] = modified_config.get('source_schema') or source_db_config.get('schema') or 'PUBLIC'
        destination_db_config = dict(settings.DATABASES[destination_conn_name])
        destination_db_config['schema'] = modified_config.get('destination_schema') or destination_db_config.get('schema') or 'PUBLIC'
        source_is_custom_sql = (modified_config.get('source_custom_sql') or '').strip() != ''
        if source_is_custom_sql:
            source_query_str = modified_config['source_custom_sql']
        else:
            source_query_str = build_dynamic_aggregation_query(source_db_config, modified_config.get('source_table'), modified_config.get('source_date_column'), modified_config.get('source_date_value'), modified_config.get('source_aggregation_type'), source_agg_column, modified_config.get('source_group_by_column'), modified_config.get('source_additional_filters'))
        destination_is_custom_sql = (modified_config.get('destination_custom_sql') or '').strip() != ''
        if destination_is_custom_sql:
            destination_query_str = modified_config['destination_custom_sql']
        else:
            destination_query_str = build_dynamic_aggregation_query(destination_db_config, modified_config.get('destination_table'), modified_config.get('destination_date_column'), modified_config.get('destination_date_value'), modified_config.get('destination_aggregation_type'), destination_agg_column, modified_config.get('destination_group_by_column'), modified_config.get('destination_additional_filters'))
        modified_config['threshold'] = 0
        modified_config['threshold_type'] = 'ABSOLUTE'
        return source_query_str, destination_query_str, source_conn_name, destination_conn_name, modified_config