import uuid
from datetime import datetime, timedelta
import logging
import json
import threading
import os
import copy
import numpy as np
import subprocess
from decimal import Decimal
from django.conf import settings
from django.db import connections

from dq_management.models import Project, TestCase, TestGroup, TestGroupTestCase, TestCaseLog, TestGroupLog
from dq_management.dq_core import build_dynamic_aggregation_query, execute_query_to_dataframe
from dq_management.test_case_manager import TestCaseProcessor
from dq_management.airflow_dag_generator import generate_dag_file

logger = logging.getLogger(__name__)

# Global dict to track active group runs
active_runs = {}

def _to_json_safe(obj):
    if isinstance(obj, dict):
        return {k: _to_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_to_json_safe(v) for v in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

def _extract_and_validate_form_data(form_data):
    errors = []
    config = {
        "project_id": form_data.get('project_id', ''),
        "test_name": form_data.get('test_name', ''),
        "category": (form_data.get('category') or '').strip(),
        "test_type": form_data.get('test_type', 'Aggregation Comparison'),
        "source_connection_source": form_data.get('source_connection_source', ''),
        "source_custom_sql": (form_data.get('source_custom_sql') or '').strip(),
        "source_table": form_data.get('source_table', ''),
        "source_date_column": form_data.get('source_date_column', ''),
        "source_date_value": (form_data.get('source_date_value') or '').strip(),
        "source_aggregation_type": form_data.get('source_aggregation_type', ''),
        "source_aggregation_column": form_data.get('source_aggregation_column', ''),
        "source_group_by_column": (form_data.get('source_group_by_column') or '').strip(),
        "source_additional_filters": (form_data.get('source_additional_filters') or '').strip(),
        "destination_connection_source": form_data.get('destination_connection_source', ''),
        "destination_custom_sql": (form_data.get('destination_custom_sql') or '').strip(),
        "destination_table": form_data.get('destination_table', ''),
        "destination_date_column": form_data.get('destination_date_column', ''),
        "destination_date_value": (form_data.get('destination_date_value') or '').strip(),
        "destination_aggregation_type": form_data.get('destination_aggregation_type', ''),
        "destination_aggregation_column": form_data.get('destination_aggregation_column', ''),
        "destination_group_by_column": (form_data.get('destination_group_by_column') or '').strip(),
        "destination_additional_filters": (form_data.get('destination_additional_filters') or '').strip(),
        "historical_periods": int(form_data.get('historical_periods', 1) or 1),
        "source_workspace_id": (form_data.get('source_workspace_id') or '').strip(),
        "source_dataset_id": (form_data.get('source_dataset_id') or '').strip(),
        "source_dax_query": (form_data.get('source_dax_query') or '').strip(),
        "destination_workspace_id": (form_data.get('destination_workspace_id') or '').strip(),
        "destination_dataset_id": (form_data.get('destination_dataset_id') or '').strip(),
        "destination_dax_query": (form_data.get('destination_dax_query') or '').strip(),
        "threshold": float(form_data.get('threshold', 0) or 0),
        "threshold_type": form_data.get('threshold_type', ''),
        "possible_resolution": (form_data.get('possible_resolution') or '').strip()
    }

    normalized_test_type = (config["test_type"] or '').strip().title()
    config["test_type"] = normalized_test_type
    if not config["test_name"]: errors.append("Test Name is required.")
    if not config["project_id"]: errors.append("Project ID is missing. Cannot save test case without an associated project.")
    
    # --- New and refactored validation logic for all test types ---
    if normalized_test_type == 'Aggregation Comparison':
        prefixes_to_validate = ['source', 'destination']
        if not config["threshold_type"]: errors.append("Threshold Type is required.")
    elif normalized_test_type == 'Power Bi Aggregation':
        prefixes_to_validate = ['destination']
        if not config['powerbi_group_id']: errors.append("Power BI Group ID is required.")
        if not config['powerbi_dataset_id']: errors.append("Power BI Dataset ID is required.")
        if not config["threshold_type"]: errors.append("Threshold Type is required.")
    elif normalized_test_type in ['Drift Test', 'Availability Test', 'Power Bi Availability', 'Power Bi Drift']:
        prefixes_to_validate = ['source']
        if normalized_test_type in ['Drift Test', 'Power Bi Drift']:
            config['threshold_type'] = 'PERCENTAGE'
            if config['threshold'] < 0: errors.append("Threshold cannot be negative for Drift Test.")
        elif normalized_test_type in ['Availability Test', 'Power Bi Availability']:
            config['threshold'] = 0
            config['threshold_type'] = 'ABSOLUTE'
        if normalized_test_type.startswith('Power Bi'):
            if not config['powerbi_group_id']: errors.append("Power BI Group ID is required.")
            if not config['powerbi_dataset_id']: errors.append("Power BI Dataset ID is required.")
            if not config['powerbi_dax_query']: errors.append("Power BI DAX Query is required for Power BI-based tests.")
    else:
        errors.append(f"Unknown or unsupported test type: '{normalized_test_type}'.")
        return config, errors

    # --- General validation for all test types using prefixes ---
    for prefix in prefixes_to_validate:
        is_custom_sql = config.get(f'{prefix}_custom_sql', '').strip() != ''
        # --- Drift Test and Availability Test: Do NOT require date_value ---
        skip_date_value_check = normalized_test_type in ['Drift Test', 'Power Bi Drift', 'Availability Test', 'Power Bi Availability']
        if not is_custom_sql:
            if not config[f'{prefix}_table']: errors.append(f"'{prefix.capitalize()} Table Name' is required when not using custom SQL.")
            if not config[f'{prefix}_date_column']: errors.append(f"'{prefix.capitalize()} Primary Date Column' is required when not using custom SQL.")
            date_value = config[f'{prefix}_date_value']
            if not date_value and not skip_date_value_check:
                errors.append(f"'{prefix.capitalize()} Primary Date Value' is required when not using custom SQL.")
            elif date_value and date_value.upper() != 'CURRENT_DATE()' and not skip_date_value_check:
                try: datetime.strptime(date_value, '%Y-%m-%d')
                except ValueError: errors.append(f"'{prefix.capitalize()} Primary Date Value' must be in YYYY-MM-DD format or 'CURRENT_DATE()'.")
            agg_type = config[f'{prefix}_aggregation_type'].strip().upper()
            agg_column = config[f'{prefix}_aggregation_column'].strip()
            if not agg_type: errors.append(f"'{prefix.capitalize()} Aggregation Type' is required when not using custom SQL.")
            elif agg_type not in ['COUNT', 'COUNT(*)', 'SUM', 'AVG', 'MIN', 'MAX']: errors.append(f"'{prefix.capitalize()} Aggregation Type' must be one of COUNT, COUNT(*), SUM, AVG, MIN, MAX.")
            elif agg_type not in ['COUNT', 'COUNT(*)'] and not agg_column:
                errors.append(f"'{prefix.capitalize()} Column for Aggregation' is required for {agg_type} type.")
        elif config.get(f'{prefix}_custom_sql', '').strip():
            pass
        else:
            errors.append(f"'{prefix.capitalize()} Custom SQL Query' cannot be empty if provided.")
        allowed_sources = ['PROD', 'DEV', 'Power BI', 'snowflake_prod', 'snowflake_dev']
        if config[f'{prefix}_connection_source'] and config[f'{prefix}_connection_source'] not in allowed_sources:
            errors.append(f"Invalid {prefix.capitalize()} Connection Source: {config[f'{prefix}_connection_source']}. Must be one of {['PROD', 'DEV', 'Power BI']}")
    return config, errors

# --- All other functions in services.py remain unchanged ---
def get_all_projects_from_db():
    projects = Project.objects.using('snowflake_dev').all().order_by('project_name')
    return list(projects.values())

def get_project_from_db(project_id):
    try:
        project = Project.objects.using('snowflake_dev').get(pk=project_id)
        project_dict = project.__dict__.copy()
        project_dict.pop('_state', None)
        # Ensure compatibility for Django template rendering
        # REMOVED: String conversion of datetime objects
        project_dict['id'] = project_dict['project_id']
        project_dict['name'] = project_dict['project_name']
        return project_dict
    except Project.DoesNotExist:
        logger.error(f"Project with ID '{project_id}' not found.")
        return None

def get_test_cases_for_project_from_db(project_id):
    test_cases_qs = TestCase.objects.using('snowflake_dev').filter(project_id=project_id).order_by('test_name')
    test_cases_list = []
    default_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    for tc in test_cases_qs:
        tc_data = tc.__dict__.copy()
        tc_data.pop('_state', None)
        tc_data['id'] = tc_data['test_case_id']
        tc_data['source_aggregation_type'] = tc_data.get('source_agg_type')
        tc_data['source_aggregation_column'] = tc_data.get('source_agg_column')
        tc_data['destination_aggregation_type'] = tc_data.get('destination_agg_type')
        tc_data['destination_aggregation_column'] = tc_data.get('destination_agg_column')
        tc_data['source_custom_sql'] = tc_data.get('custom_source_sql', '')
        tc_data['destination_custom_sql'] = tc_data.get('custom_destination_sql', '')
        tc_data['source_additional_filters'] = tc_data.get('additional_source_filters', '')
        tc_data['destination_additional_filters'] = tc_data.get('additional_destination_filters', '')
        tc_data['source_date_value'] = (tc_data.get('source_date_value') or '').strip() or default_date_str
        tc_data['destination_date_value'] = (tc_data.get('destination_date_value') or '').strip() or default_date_str
        tc_data['possible_resolution'] = tc_data.get('possible_resolution', '')
        if not tc_data.get('source_custom_sql'):
            tc_data['source_aggregation_type'] = tc_data.get('source_agg_type') or 'COUNT(*)'
        if not tc_data.get('destination_custom_sql'):
            tc_data['destination_aggregation_type'] = tc_data.get('destination_agg_type') or 'COUNT(*)'
        # Ensure compatibility for Django template rendering
        # REMOVED: String conversion of datetime objects
        test_cases_list.append(tc_data)
    return test_cases_list

def get_test_case_details_from_db(test_case_id):
    try:
        test_case = TestCase.objects.using('snowflake_dev').get(pk=test_case_id)
        test_case_data = test_case.__dict__.copy()
        test_case_data.pop('_state', None)
        test_case_data['id'] = test_case_data['test_case_id']
        test_case_data['source_aggregation_type'] = test_case_data.get('source_agg_type')
        test_case_data['source_aggregation_column'] = test_case_data.get('source_agg_column')
        test_case_data['destination_aggregation_type'] = test_case_data.get('destination_agg_type')
        test_case_data['destination_aggregation_column'] = test_case_data.get('destination_agg_column')
        test_case_data['source_custom_sql'] = test_case_data.get('custom_source_sql', '')
        test_case_data['destination_custom_sql'] = test_case_data.get('custom_destination_sql', '')
        test_case_data['source_additional_filters'] = test_case_data.get('additional_source_filters', '')
        test_case_data['destination_additional_filters'] = test_case_data.get('additional_destination_filters', '')
        default_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        test_case_data['source_date_value'] = (test_case_data.get('source_date_value') or '').strip() or default_date_str
        test_case_data['destination_date_value'] = (test_case_data.get('destination_date_value') or '').strip() or default_date_str
        test_case_data['destination_workspace_id'] = test_case_data.get('destination_workspace_id', '')
        test_case_data['destination_dataset_id'] = test_case_data.get('destination_dataset_id', '')
        test_case_data['destination_dax_query'] = test_case_data.get('destination_dax_query', '')
        test_case_data['source_workspace_id'] = test_case_data.get('source_workspace_id', '')
        test_case_data['source_dataset_id'] = test_case_data.get('source_dataset_id', '')
        test_case_data['source_dax_query'] = test_case_data.get('source_dax_query', '')
        test_case_data['possible_resolution'] = test_case_data.get('possible_resolution', '')
        if not test_case_data.get('source_custom_sql'):
            test_case_data['source_aggregation_type'] = test_case_data.get('source_agg_type') or 'COUNT(*)'
        if not test_case_data.get('destination_custom_sql'):
            test_case_data['destination_aggregation_type'] = test_case_data.get('destination_agg_type') or 'COUNT(*)'
        # Ensure compatibility for Django template rendering
        # REMOVED: String conversion of datetime objects
        return test_case_data
    except TestCase.DoesNotExist:
        logger.error(f"Test case with ID '{test_case_id}' not found.")
        return None
    except Exception as e:
        logger.exception(f"Error fetching test case {test_case_id} details from Snowflake:")
        return None


def save_project_to_db(project_id, project_name, project_description, priority, created_by):
    """
    Creates a new Project or updates an existing one using Django ORM.
    """
    try:
        # Check if project_id exists (for update) or use it for create
        project_obj = Project.objects.using('snowflake_dev').filter(project_id=project_id).first()
        
        if project_obj:
            # --- Update existing project ---
            project_obj.project_name = project_name
            project_obj.project_description = project_description
            project_obj.priority = priority
            # updated_at will be set automatically by auto_now=True
            project_obj.save(using='snowflake_dev')
            logger.info(f"Project {project_id} updated.")
        else:
            # --- Create new project ---
            Project.objects.using('snowflake_dev').create(
                project_id=project_id,
                project_name=project_name,
                project_description=project_description,
                priority=priority,
                created_by=created_by,
                # created_at/updated_at are set automatically
            )
            logger.info(f"New project {project_id} created.")

        return True
    except Exception as e:
        logger.error(f"Error saving project {project_id}: {e}", exc_info=True)
        return False

def delete_project_from_db(project_id):
    """
    Deletes a project and all associated test cases and test groups.
    """
    try:
        # First, delete all test groups associated with the project
        test_groups = TestGroup.objects.using('snowflake_dev').filter(project_id=project_id)
        for group in test_groups:
            # Delete associated test group test cases
            TestGroupTestCase.objects.using('snowflake_dev').filter(test_group=group).delete()
            # Delete the DAG file for the test group
            group_id_safe = group.test_group_id.replace("-", "_")
            dags_folder_path = getattr(settings, "AIRFLOW_DAGS_DIR", None)
            dag_file_name = f"test_group_{group_id_safe}.py"
            dag_file_path = os.path.join(dags_folder_path, dag_file_name) if dags_folder_path else None
            if dag_file_path and os.path.isfile(dag_file_path):
                try:
                    os.remove(dag_file_path)
                    logger.info(f"Deleted DAG file {dag_file_path} for group {group.test_group_id}.")
                except Exception as file_error:
                    logger.error(f"Error deleting DAG file for group {group.test_group_id}: {file_error}")
            # Delete the test group itself
            group.delete()

        # Delete all test cases associated with the project
        TestCase.objects.using('snowflake_dev').filter(project_id=project_id).delete()

        # Finally, delete the project
        project = Project.objects.using('snowflake_dev').get(project_id=project_id)
        project.delete()

        logger.info(f"Project {project_id} and all associated data deleted successfully.")
        return True, f"Project '{project.project_name}' and all associated data deleted successfully."
    except Exception as e:
        logger.error(f"Error deleting project {project_id}: {e}")
        return False, f"Failed to delete project: {e}"


def save_test_case_to_db(test_case_id, project_id, config):
    try:
        # If test_case_id is blank or None, generate a new one (for new test cases)
        if not test_case_id:
            test_case_id = str(uuid.uuid4())
            created = True
        else:
            # Check if test_case exists
            try:
                test_case = TestCase.objects.using('snowflake_dev').get(test_case_id=test_case_id)
                created = False
            except TestCase.DoesNotExist:
                created = True

        test_case, _ = TestCase.objects.using('snowflake_dev').update_or_create(
            test_case_id=test_case_id,
            defaults={
                'project_id': project_id,
                'test_name': config['test_name'],
                'category': config.get('category', ''),
                'test_type': config.get('test_type', 'Aggregation Comparison'),
                'source_connection_source': config.get('source_connection_source', ''),
                'destination_connection_source': config.get('destination_connection_source', ''),
                'source_table': config.get('source_table', ''),
                'destination_table': config.get('destination_table', ''),
                'additional_source_filters': config.get('source_additional_filters', ''),
                'additional_destination_filters': config.get('additional_destination_filters', ''),
                'custom_source_sql': config.get('source_custom_sql', ''),
                'custom_destination_sql': config.get('destination_custom_sql', ''),
                'threshold': config.get('threshold'),
                'threshold_type': config.get('threshold_type', ''),
                'status': config.get('status', 'DRAFT'),
                'created_by': config.get('created_by', 'system_user'),
                'source_date_column': config.get('source_date_column', ''),
                'source_agg_type': config.get('source_aggregation_type', ''),
                'source_agg_column': config.get('source_aggregation_column', ''),
                'source_group_by_column': config.get('source_group_by_column', ''),
                'destination_date_column': config.get('destination_date_column', ''),
                'destination_agg_type': config.get('destination_aggregation_type', ''),
                'destination_agg_column': config.get('destination_aggregation_column', ''),
                'destination_group_by_column': config.get('destination_group_by_column', ''),
                'source_date_value': config.get('source_date_value', ''),
                'destination_date_value': config.get('destination_date_value', ''),
                'possible_resolution': config.get('possible_resolution', ''),
                'source_workspace_id': config.get('source_workspace_id', ''),
                'source_dataset_id': config.get('source_dataset_id', ''),
                'source_dax_query': config.get('source_dax_query', ''),
                'destination_workspace_id': config.get('destination_workspace_id', ''),
                'destination_dataset_id': config.get('destination_dataset_id', ''),
                'destination_dax_query': config.get('destination_dax_query', ''),
            }
        )
        if created:
            logger.info(f"Test case '{config['test_name']}' ({test_case_id}) inserted into Snowflake.")
        else:
            logger.info(f"Test case '{config['test_name']}' ({test_case_id}) updated in Snowflake.")
        return True
    except Exception as e:
        logger.exception(f"Error saving test case {test_case_id} to Snowflake:")
        return False

def delete_test_case_from_db(test_case_id):
    try:
        # First, delete any associations in the junction table
        TestGroupTestCase.objects.using('snowflake_dev').filter(test_case_id=test_case_id).delete()
        logger.info(f"Removed test case {test_case_id} from all test groups.")

        test_case = TestCase.objects.using('snowflake_dev').get(pk=test_case_id)
        project_id = test_case.project_id
        test_case.delete()
        logger.info(f"Test case {test_case_id} deleted successfully.")
        return True, "Test case deleted successfully.", project_id
    except TestCase.DoesNotExist:
        logger.error(f"Test case {test_case_id} not found.")
        return False, "Test case not found.", None
    except Exception as e:
        logger.error(f"Error deleting test case {test_case_id}: {e}")
        return False, f"Failed to delete test case: {e}", None

def get_test_groups_for_project_from_db(project_id):
    test_groups_qs = TestGroup.objects.using('snowflake_dev').filter(project_id=project_id).order_by('group_name')
    test_groups_list = []
    for tg in test_groups_qs:
        tg_data = tg.__dict__.copy()
        tg_data.pop('_state', None)
        tg_data['id'] = tg_data['test_group_id']
        tg_data['name'] = tg_data['group_name']
        # Ensure compatibility for Django template rendering
        # Convert datetime objects to strings for JSON serialization
        for dt_col in ['created_at', 'updated_at', 'last_run']:
            if isinstance(tg_data.get(dt_col), datetime):
                tg_data[dt_col] = tg_data[dt_col].strftime('%Y-%m-%d %H:%M:%S')
            else:
                tg_data[dt_col] = 'N/A'
        test_groups_list.append(tg_data)
    return test_groups_list

def get_test_group_details_from_db(group_id):
    try:
        group = TestGroup.objects.using('snowflake_dev').get(pk=group_id)
        group_dict = group.__dict__.copy()
        group_dict.pop('_state', None)
        group_dict['id'] = group_dict['test_group_id']
        group_dict['name'] = group_dict['group_name']
        # Convert datetime objects to strings for JSON serialization
        for dt_col in ['created_at', 'updated_at', 'last_run']:
            if isinstance(group_dict.get(dt_col), datetime):
                group_dict[dt_col] = group_dict[dt_col].strftime('%Y-%m-%d %H:%M:%S')
            else:
                group_dict[dt_col] = 'N/A'
        return group_dict
    except TestGroup.DoesNotExist:
        logger.error(f"Test group with ID '{group_id}' not found.")
        return None
    except Exception as e:
        logger.exception(f"Error fetching test group {group_id} details from Snowflake:")
        return None

def save_test_group_to_db(group_id, project_id, group_name, group_description, schedule_cron, created_by, selected_test_cases_data):
    try:
        total_tests_count = len(selected_test_cases_data) if selected_test_cases_data else 0
        test_group, created = TestGroup.objects.using('snowflake_dev').update_or_create(
            test_group_id=group_id,
            defaults={
                'project_id': project_id, 'group_name': group_name,
                'group_description': group_description, 'schedule_cron': schedule_cron,
                'created_by': created_by,
                'total_tests': total_tests_count, # Update total tests count on save
            }
        )
        if created:
            logger.info(f"Inserted new test group {group_id}.")
        else:
            logger.info(f"Updated test group {group_id} metadata.")
        TestGroupTestCase.objects.using('snowflake_dev').filter(test_group=test_group).delete()
        logger.info(f"Cleared old test cases for group {group_id}.")
        if selected_test_cases_data:
            new_junction_records = [
                TestGroupTestCase(
                    test_group=test_group, test_case_id=item['test_case_id'],
                    execution_order=item['execution_order']
                ) for item in selected_test_cases_data
            ]
            TestGroupTestCase.objects.using('snowflake_dev').bulk_create(new_junction_records)
            logger.info(f"Inserted {len(new_junction_records)} test cases for group {group_id}.")
        test_case_ids = [
            item['test_case_id']
            for item in sorted(selected_test_cases_data, key=lambda x: x['execution_order'])
        ] if selected_test_cases_data else []
        output_dir = settings.EAGLE_DAGS_DIR
        if test_case_ids:
            try:
                generate_dag_file(group_id, test_case_ids, schedule_cron)
            except Exception as dag_error:
                logger.exception(f"Error generating DAG file for test group {group_id}: {dag_error}")
        else:
            logger.warning(f"No test cases for group {group_id}; no DAG file generated.")
        return True
    except Exception as e:
        logger.exception(f"Error saving test group {group_id} Snowflake:")
        return False

def delete_test_group_from_db(group_id):
    try:
        group = TestGroup.objects.using('snowflake_dev').get(pk=group_id)
        project_id = group.project_id
        group.delete()
        group_id_safe = group_id.replace("-", "_")
        dags_folder_path = getattr(settings, "AIRFLOW_DAGS_DIR", None)
        dag_file_name = f"test_group_{group_id_safe}.py"
        dag_file_path = os.path.join(dags_folder_path, dag_file_name) if dags_folder_path else None
        if dag_file_path and os.path.isfile(dag_file_path):
            try:
                os.remove(dag_file_path)
                logger.info(f"Deleted DAG file {dag_file_path} for group {group_id}.")
            except Exception as file_error:
                logger.error(f"Error deleting DAG file: {file_error}")
        else:
            logger.info(f"DAG file {dag_file_path} not found for group {group_id}, skipping file deletion.")
        return True, "Test group, its test cases, and DAG file deleted successfully.", project_id
    except TestGroup.DoesNotExist:
        logger.error(f"Test group {group_id} not found.")
        return False, "Test group not found.", None
    except Exception as e:
        logger.exception(f"CRITICAL ERROR: Failed to delete test group {group_id} from Snowflake.")
        return False, f"Failed to delete test group: {e}", None

def get_test_cases_in_group_from_db(group_id):
    """
    Fetches test cases for a specific test group, ordered by execution order.
    """
    fetched_test_cases = []
    try:
        # Explicitly reference the correct fields
        junction_records = TestGroupTestCase.objects.using('snowflake_dev').filter(
            test_group_id=group_id
        ).select_related('test_case').order_by('execution_order')  # Ensure 'execution_order' exists in the table

        default_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        for record in junction_records:
            tc_data_orm = record.test_case
            tc_data = {
                "test_case_id": tc_data_orm.test_case_id,  # Ensure this column exists
                "project_id": tc_data_orm.project_id,
                "test_name": tc_data_orm.test_name,
                "test_type": tc_data_orm.test_type,
                "source_connection_source": tc_data_orm.source_connection_source,
                "destination_connection_source": tc_data_orm.destination_connection_source,
                "source_table": tc_data_orm.source_table,
                "destination_table": tc_data_orm.destination_table,
                "source_date_column": tc_data_orm.source_date_column,
                "source_agg_type": tc_data_orm.source_agg_type,
                "source_agg_column": tc_data_orm.source_agg_column,
                "source_group_by_column": tc_data_orm.source_group_by_column,
                "destination_date_column": tc_data_orm.destination_date_column,
                "destination_agg_type": tc_data_orm.destination_agg_type,
                "destination_agg_column": tc_data_orm.destination_agg_column,
                "destination_group_by_column": tc_data_orm.destination_group_by_column,
                "additional_source_filters": tc_data_orm.additional_source_filters,
                "additional_destination_filters": tc_data_orm.additional_destination_filters,
                "custom_source_sql": tc_data_orm.custom_source_sql,
                "custom_destination_sql": tc_data_orm.custom_destination_sql,
                "source_date_value": (tc_data_orm.source_date_value or '').strip() or default_date_str,
                "destination_date_value": (tc_data_orm.destination_date_value or '').strip() or default_date_str,
                "threshold": tc_data_orm.threshold,
                "threshold_type": tc_data_orm.threshold_type,
                "status": tc_data_orm.status,
                "created_by": tc_data_orm.created_by,
                "created_at": tc_data_orm.created_at,
                "updated_at": tc_data_orm.updated_at,
            }
            if not tc_data.get('custom_source_sql'):
                tc_data['source_agg_type'] = tc_data.get('source_agg_type') or 'COUNT(*)'
            if not tc_data.get('destination_custom_sql'):
                tc_data['destination_agg_type'] = tc_data.get('destination_agg_type') or 'COUNT(*)'
            # Convert datetime objects to strings for JSON serialization
            for dt_col in ['created_at', 'updated_at']:
                if isinstance(tc_data.get(dt_col), datetime):
                    tc_data[dt_col] = tc_data[dt_col].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    tc_data[dt_col] = 'N/A'
            fetched_test_cases.append({
                "id": tc_data['test_case_id'],
                "execution_order": record.execution_order,  # Ensure this column exists
                "meta": {
                    "id": tc_data['test_case_id'],
                    "name": tc_data['test_name'],
                    "type": tc_data['test_type'],
                    "project_id": tc_data['project_id'],
                    "status": tc_data['status']
                },
                "detail": tc_data
            })
    except Exception as e:
        logger.error(f"Error in get_test_cases_in_group_from_db: {e}")
    return fetched_test_cases

def get_test_case_logs_from_db():
    logs_qs = TestCaseLog.objects.using('snowflake_dev').all().order_by('-run_timestamp')
    logs_list = []
    for log_entry in logs_qs:
        log_data = log_entry.__dict__.copy()
        log_data.pop('_state', None)
        # project_name is already stored in the model
        for key in ['source_value', 'destination_value', 'difference']:
            if isinstance(log_data.get(key), str):
                try:
                    log_data[key] = json.loads(log_data[key])
                except json.JSONDecodeError:
                    pass
        logs_list.append(log_data)
    return logs_list

def get_test_group_logs_from_db():
    logs_qs = TestGroupLog.objects.using('snowflake_dev').all().order_by('-start_timestamp')
    logs_list = []
    for log_entry in logs_qs:
        log_data = log_entry.__dict__.copy()
        log_data.pop('_state', None)
        # group_name and project_name are already stored as snapshots in the model
        if isinstance(log_data.get('results_details'), str):
            try:
                log_data['results_details'] = json.loads(log_data['results_details'])
            except json.JSONDecodeError:
                pass
        logs_list.append(log_data)
    return logs_list

def log_test_case_result_orm(run_id, test_case_id, project_id, test_name, test_type,
                             run_status, run_message, source_value, destination_value,
                             difference, threshold_type, threshold_value, source_query, destination_query,
                             source_connection_used, destination_connection_used, parent_run_id=None, possible_resolution=None):
    try:
        project_name = None
        criticality = None
        if project_id:
            try:
                project_obj = Project.objects.using('snowflake_dev').get(project_id=project_id)
                project_name = project_obj.project_name
                criticality = project_obj.criticality_level
            except Project.DoesNotExist:
                project_name = None
                criticality = None
        TestCaseLog.objects.using('snowflake_dev').create(
            run_id=run_id, test_case_id=test_case_id, project_id=project_id,
            project_name=project_name, criticality=criticality, test_name=test_name, test_type=test_type,
            run_status=run_status, run_message=run_message, source_value=_to_json_safe(source_value),
            destination_value=_to_json_safe(destination_value), difference=_to_json_safe(difference),
            threshold_type=str(threshold_type) if threshold_type is not None else None,
            threshold_value=str(threshold_value) if threshold_value is not None else None,
            source_query=source_query, destination_query=destination_query,
            source_connection_used=source_connection_used, destination_connection_used=destination_connection_used,
            parent_run_id=parent_run_id, possible_resolution=possible_resolution, run_timestamp=datetime.now()
        )
        logger.info(f"Test case log for {run_id} created successfully.")
    except Exception as e:
        logger.exception(f"Error creating TestCaseLog for run {run_id}: {e}")

def log_test_group_status_orm(run_id, test_group_id, group_name, project_id, project_name, status, message, results_details, start_time, end_time=None):
    try:
        criticality = None
        if project_id:
            try:
                project_obj = Project.objects.using('snowflake_dev').get(project_id=project_id)
                criticality = project_obj.criticality_level
            except Project.DoesNotExist:
                criticality = None
        TestGroupLog.objects.using('snowflake_dev').update_or_create(
            run_id=run_id,
            defaults={
                'test_group_id': test_group_id, 'group_name': group_name,
                'project_id': project_id, 'project_name': project_name, 'criticality': criticality,
                'status': status, 'message': message, 'results_details': _to_json_safe(results_details),
                'start_timestamp': start_time, 'end_timestamp': end_time
            }
        )
        logger.info(f"Test group log for {run_id} updated successfully.")
    except Exception as e:
        logger.exception(f"Error creating/updating TestGroupLog for run {run_id}: {e}")

def run_adhoc_test_logic(test_case_id, parent_run_id=None):
    run_id = str(uuid.uuid4())
    result = {
        "run_id": run_id, "test_case_id": test_case_id, "status": "ERROR",
        "message": "Test execution failed.", "status_code": 500, "project_id": None,
        "test_name": None, "test_type": None, "source_value": None,
        "destination_value": None, "difference": None, "threshold": None,
        "threshold_type": None, "source_query": None, "destination_query": None,
        "source_connection_used": None, "destination_connection_used": None,
    }
    try:
        test_case_data = get_test_case_details_from_db(test_case_id)
        if not test_case_data:
            result["message"] = "Test case not found."
            result["status_code"] = 404
            return result
        result["project_id"] = test_case_data.get("project_id")
        result["test_name"] = test_case_data.get("test_name")
        result["test_type"] = test_case_data.get("test_type")
        project_name = None
        criticality = None
        if result["project_id"]:
            try:
                project_obj = Project.objects.using('snowflake_dev').get(project_id=result["project_id"])
                project_name = project_obj.project_name
                criticality = getattr(project_obj, 'criticality_level', None)
            except Project.DoesNotExist:
                project_name = None
                criticality = None
        result["project_name"] = project_name
        result["criticality"] = criticality
        config, validation_errors = _extract_and_validate_form_data(test_case_data)
        if validation_errors:
            result["message"] = "Validation error: " + "; ".join(validation_errors)
            result["details"] = validation_errors
            return result
        processor = TestCaseProcessor()
        processor_result = processor.process_test_request(config, action='run', run_id=run_id)
        result.update(processor_result)
        result["status_code"] = 200 if result.get("status") == "PASS" else 500

        # --- Update TestCase status in DB after adhoc run ---
        try:
            TestCase.objects.using('snowflake_dev').filter(test_case_id=test_case_id).update(status=result.get("status"))
        except Exception as update_e:
            logger.error(f"Failed to update status for test case {test_case_id}: {update_e}")

    except Exception as e:
        logger.exception(f"Error running ad-hoc test for {test_case_id}: {e}")
        result["message"] = f"Exception: {e}"
    try:
        TestCaseLog.objects.using('snowflake_dev').create(
            run_id=result.get('run_id'), test_case_id=result.get('test_case_id'),
            project_id=result.get('project_id'), project_name=result.get('project_name'),
            criticality=result.get('criticality'),
            test_name=result.get('test_name'), test_type=result.get('test_type'),
            run_status=result.get('status'), run_message=result.get('message'),
            source_value=_to_json_safe(result.get('source_value')),
            destination_value=_to_json_safe(result.get('destination_value')),
            difference=_to_json_safe(result.get('difference')),
            threshold_type=str(result.get('threshold_type')) if result.get('threshold_type') is not None else None,
            threshold_value=str(result.get('threshold')) if result.get('threshold') is not None else None,
            source_query=result.get('source_query'), destination_query=result.get('destination_query'),
            source_connection_used=result.get('source_connection_used'),
            destination_connection_used=result.get('destination_connection_used'),
            parent_run_id=parent_run_id, possible_resolution=test_case_data.get('possible_resolution', '')
        )
    except Exception as log_e:
        logger.exception(f"Error logging ad-hoc test run for {test_case_id}: {log_e}")
        result['log_error'] = str(log_e)
    return _to_json_safe(result)

def schedule_test_group_logic(group_id):
    try:
        # Retrieve test group from DB to ensure it exists
        group = get_test_group_details_from_db(group_id)
        if not group:
            logger.error(f"Test group {group_id} not found.")
            return {"status": "ERROR", "message": f"Test group {group_id} not found."}

        # Construct DAG ID
        group_id_safe = group_id.replace("-", "_")
        dag_id = f"test_group_{group_id_safe}"

        # Trigger the DAG using Airflow CLI
        logger.info(f"Triggering Airflow DAG {dag_id} for test group {group_id}.")
        result = subprocess.run(
            ["airflow", "dags", "trigger", dag_id],
            capture_output=True,
            text=True,
            cwd=settings.BASE_DIR  # Run from project root if needed
        )

        if result.returncode == 0:
            logger.info(f"Successfully triggered DAG {dag_id} for test group {group_id}.")
            return {"status": "SUCCESS", "message": f"Test group {group_id} scheduled successfully. DAG {dag_id} triggered."}
        else:
            logger.error(f"Failed to trigger DAG {dag_id} for test group {group_id}. Stdout: {result.stdout}, Stderr: {result.stderr}")
            return {"status": "ERROR", "message": f"Failed to schedule test group {group_id}. Error: {result.stderr.strip()}"}

    except Exception as e:
        logger.exception(f"Error scheduling test group {group_id}: {e}")
        return {"status": "ERROR", "message": f"Exception occurred while scheduling test group {group_id}: {str(e)}"}

def get_available_connection_sources():
    sources = list(settings.DATABASES.keys())
    # Add Power BI as a selectable connection source for dropdowns
    if hasattr(settings, "POWERBI_CREDENTIALS"):
        sources.append("Power BI")
    return sources
    
GROUP_RUN_STATUS = {}

def start_group_run_task(group_id):
    run_id = str(uuid.uuid4())
    GROUP_RUN_STATUS[run_id] = {
        "status": "PENDING", "total_test_cases": 0, "executed_count": 0,
        "current_test_name": "Preparing to run...", "results": []
    }
    thread = threading.Thread(target=_execute_group_in_background, args=(run_id, group_id))
    thread.daemon = True
    thread.start()
    logger.info(f"Started background run for group {group_id} with run ID: {run_id}")
    return run_id

def get_group_run_status(run_id):
    return GROUP_RUN_STATUS.get(run_id, {"status": "NOT_FOUND", "message": "Run ID not found."})

def _execute_group_in_background(run_id, group_id):
    overall_group_status = "PASS"
    failed_tests_count = 0
    all_test_results = []
    start_time = datetime.now()
    total_tests = 0
    try:
        group_meta = get_test_group_details_from_db(group_id)
        if not group_meta:
            GROUP_RUN_STATUS[run_id]["status"] = "ERROR"
            GROUP_RUN_STATUS[run_id]["current_test_name"] = "Group not found."
            logger.error(f"Test group {group_id} not found for background run.")
            return
        project_name = None
        if group_meta.get('project_id'):
            try:
                project_obj = Project.objects.using('snowflake_dev').get(project_id=group_meta['project_id'])
                project_name = project_obj.project_name
            except Project.DoesNotExist:
                project_name = None
        test_cases_in_group = get_test_cases_in_group_from_db(group_id)
        total_tests = len(test_cases_in_group)
        GROUP_RUN_STATUS[run_id]["total_test_cases"] = total_tests
        GROUP_RUN_STATUS[run_id]["status"] = "RUNNING"
        logger.info(f"Starting background run for group {group_id}. Total tests: {total_tests}")
        log_test_group_status_orm(
            run_id=run_id, test_group_id=group_id, group_name=group_meta['name'],
            project_id=group_meta['project_id'], project_name=project_name,
            status="RUNNING", message="Test group run started.",
            results_details={"test_cases_in_group": total_tests},
            start_time=start_time
        )
        for index, item in enumerate(sorted(test_cases_in_group, key=lambda x: x['execution_order'])):
            tc_id = item['id']
            tc_detail = item['detail']
            tc_name = tc_detail.get('test_name', 'N/A')
            GROUP_RUN_STATUS[run_id]["executed_count"] = index + 1
            GROUP_RUN_STATUS[run_id]["current_test_name"] = tc_name
            logger.info(f"     - Executing test {index + 1}/{total_tests}: '{tc_name}' ({tc_id})")
            result = {"status": "ERROR", "message": "Test execution failed unexpectedly or was not fully processed."} 
            try:
                result = run_adhoc_test_logic(tc_id, parent_run_id=run_id)
            except Exception as e:
                logger.exception(f"     -> Error running test case {tc_id} in group {group_id}: {e}")
                result = {"status": "ERROR", "message": f"Execution failed: {e}"}
            all_test_results.append(result)
            logger.info(f"     -> Test '{tc_name}' status: {result.get('status')}")
            if result.get('status') == 'FAIL':
                failed_tests_count += 1
                if overall_group_status == "PASS":
                    overall_group_status = "FAIL"
            elif result.get('status') == 'ERROR':
                overall_group_status = "ERROR"
        GROUP_RUN_STATUS[run_id]["results"] = all_test_results
    except Exception as e:
        logger.exception(f"CRITICAL ERROR: Failed to run group {group_id}: {e}")
        overall_group_status = "ERROR"
    finally:
        end_time = datetime.now()
        final_message = f"Test group run finished with status: {overall_group_status}."
        detailed_results = {
            "Overall Status": overall_group_status, "Total Tests": total_tests,
            "Failed Tests": failed_tests_count,
            "Failed Test Cases": [
                {
                    "test_case_id": res.get('test_case_id'), "test_name": res.get('test_name', 'N/A'),
                    "status": res.get('status'), "message": res.get('message')
                }
                for res in all_test_results if res.get("status") in ("FAIL", "ERROR")
            ]
        }
        log_test_group_status_orm(
            run_id=run_id, test_group_id=group_id, group_name=group_meta['name'],
            project_id=group_meta['project_id'], project_name=project_name,
            status=overall_group_status, message=final_message,
            results_details=json.dumps(detailed_results, indent=4), start_time=start_time, end_time=end_time
        )
        # --- Update the TestGroup model instance with the latest run stats ---
        try:
            TestGroup.objects.using('snowflake_dev').filter(test_group_id=group_id).update(
                last_run=end_time,
                status=overall_group_status,
                total_tests=total_tests,
                failed_tests=failed_tests_count
            )
            logger.info(f"Updated TestGroup {group_id} with final status and stats.")
        except Exception as e:
            logger.error(f"Failed to update TestGroup {group_id} with stats: {e}")

        GROUP_RUN_STATUS[run_id]["status"] = "COMPLETED"
        GROUP_RUN_STATUS[run_id]["final_status"] = overall_group_status
        GROUP_RUN_STATUS[run_id]["failed_tests"] = failed_tests_count
        logger.info(f"Background run for group {group_id} finished.")

