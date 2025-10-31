# dq_management/airflow_dag_generator.py

from jinja2 import Template
from pathlib import Path
from django.conf import settings # Use Django settings for the DAGs directory

# This Django setup code is NOT needed here, as this file is run by the Django webserver.
# The generated DAGs will have their own setup code.
# import django
# import os
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'eagle_project.settings')
# django.setup()


DAG_TEMPLATE = """
# This part is the generated DAG file that Airflow will execute

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import json
import uuid

# --- CRITICAL: Initialize Django within the DAG file ---
# Each Airflow worker needs to set up the Django environment
import os
import django
from django.conf import settings as django_settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'eagle_dq_project.settings')
django.setup()
# --- END Django Initialization ---

# Import the refactored, ORM-based functions from your Django services
from eagle_dq_project.dq_management.services import (
    run_adhoc_test_logic,
    log_test_case_result_orm,
    log_test_group_status_orm
)
from dq_management.models import TestGroupLog, TestCaseLog, TestGroup, TestCase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# This is a helper function specifically for Airflow to create initial group log record.
def start_group_logging(test_group_id, **kwargs):
    parent_run_id = kwargs.get('dag_run').run_id
    try:
        TestGroupLog.objects.using('snowflake_dev').create(
            run_id=parent_run_id,
            test_group_id=test_group_id,
            status='RUNNING',
            message='Test group run started by Airflow.',
            results_details={"test_cases_in_group": 0},
            start_timestamp=datetime.now()
        )
        logger.info(f"Initial group log created for run_id: {parent_run_id}, group_id: {test_group_id}")
    except Exception as e:
        logger.exception("An error occurred during initial group status logging.")

# This is the function that will run on the Airflow worker for individual test cases
def run_adhoc_test_task_callable(test_case_id, **kwargs):
    # Call the ORM-based test logic from your services
    parent_run_id = kwargs.get('dag_run').run_id
    run_adhoc_test_logic(test_case_id=test_case_id, parent_run_id=parent_run_id)


# This function logs the final status of the entire group
def log_final_group_status(test_group_id, **kwargs):
    parent_run_id = kwargs.get('dag_run').run_id
    try:
        test_case_logs = TestCaseLog.objects.using('snowflake_dev').filter(parent_run_id=parent_run_id)
        
        total_tests = test_case_logs.count()
        failed_tests_count = test_case_logs.filter(run_status__in=['FAIL', 'ERROR']).count()

        if test_case_logs.filter(run_status='ERROR').exists():
            final_status = 'ERROR'
        elif test_case_logs.filter(run_status='FAIL').exists():
            final_status = 'FAIL'
        else:
            final_status = 'PASS'
            
        final_message = f"Group run completed. Overall Status: {final_status}. Failed tests: {failed_tests_count} out of {total_tests}."
        
        failed_test_cases_list = []
        if failed_tests_count > 0:
            for log_entry in test_case_logs.filter(run_status__in=['FAIL', 'ERROR']):
                failed_test_cases_list.append({
                    "test_case_id": log_entry.test_case_id,
                    "test_name": log_entry.test_name,
                    "status": log_entry.run_status,
                    "message": log_entry.run_message,
                })

        results_details = {
            "Overall Status": final_status,
            "Total Tests": total_tests,
            "Failed Tests": failed_tests_count,
            "Failed Test Cases": failed_test_cases_list
        }

        TestGroupLog.objects.using('snowflake_dev').filter(run_id=parent_run_id).update(
            status=final_status,
            message=final_message,
            results_details=results_details,
            end_timestamp=datetime.now()
        )
        logger.info(f"Final group log updated for run_id: {parent_run_id}, group_id: {test_group_id} with status: {final_status}")
    except Exception as e:
        logger.exception(f"Error logging final group status for run_id: {parent_run_id}, group_id: {test_group_id}: {e}")


with DAG(
    dag_id="test_group_{{ test_group_id.replace('-', '_') }}",
    schedule_interval="{{ schedule_cron }}",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    start_logging_task = PythonOperator(
        task_id="start_group_logging",
        python_callable=start_group_logging,
        op_kwargs={'test_group_id': '{{ test_group_id }}'},
    )
    
    test_case_tasks = []
    {% for test_case_id in test_cases %}
    t{{ loop.index }} = PythonOperator(
        task_id="case_{{ loop.index }}",
        python_callable=run_adhoc_test_task_callable,
        op_kwargs={'test_case_id': "{{ test_case_id }}"},
    )
    test_case_tasks.append(t{{ loop.index }})
    {% endfor %}
    
    log_final_status_task = PythonOperator(
        task_id="log_final_group_status",
        python_callable=log_final_group_status,
        op_kwargs={'test_group_id': '{{ test_group_id }}'},
        trigger_rule='all_done',
    )

    {% if test_cases|length > 0 %}
    start_logging_task >> test_case_tasks >> log_final_status_task
    {% else %}
    start_logging_task >> log_final_status_task
    {% endif %}
"""

def generate_dag_file(test_group_id, test_case_ids, schedule_cron):
    """
    Generates an Airflow DAG file for a given test group.
    The output directory is now retrieved from Django's settings.
    """
    t = Template(DAG_TEMPLATE)
    dag_code = t.render(test_group_id=test_group_id, test_cases=test_case_ids, schedule_cron=schedule_cron)
    fname = f"test_group_{test_group_id.replace('-', '_')}.py"
    
    # Get the output directory from settings
    output_dir = settings.EAGLE_DAGS_DIR
    
    Path(output_dir).joinpath(fname).write_text(dag_code)   