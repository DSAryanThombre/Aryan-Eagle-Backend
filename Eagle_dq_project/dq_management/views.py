import os
import uuid
from datetime import datetime, timedelta
import json
import numpy as np
import logging
from graphviz import Digraph
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from django.contrib import messages
from django.http import JsonResponse
from django.conf import settings
from django.core.paginator import Paginator

from .forms import ProjectForm

from .models import Project, TestCase

from .services import (
    _extract_and_validate_form_data,
    get_all_projects_from_db,
    get_project_from_db,
    get_test_cases_for_project_from_db,
    get_test_case_details_from_db,
    save_test_case_to_db,
    get_test_groups_for_project_from_db,
    get_test_group_details_from_db,
    save_test_group_to_db,
    save_project_to_db,
    get_test_cases_in_group_from_db,
    run_adhoc_test_logic,
    delete_test_case_from_db,
    start_group_run_task,
    get_group_run_status,
    delete_test_group_from_db,
    delete_project_from_db,
    get_test_case_logs_from_db,
    get_test_group_logs_from_db,
    schedule_test_group_logic,
    get_available_connection_sources,
    _to_json_safe  # Import the helper function
)
from .chart_utils import make_criticality_bar_chart
from .models import TestGroupLog
from dq_management.test_case_manager import TestCaseProcessor # Still needed for direct instantiation

logger = logging.getLogger(__name__)

def create_project(request):
    """Handles project creation (GET request to show form, POST request to save)."""
    
    # 1. Handle POST Request (Form Submission)
    if request.method == 'POST':
        form = ProjectForm(request.POST)
        form.instance._state.db = 'snowflake_dev'  # Ensure form validation uses the correct database
        
        if form.is_valid():
            try:
                # Get an unsaved instance of the model object from the form
                new_project = form.save(commit=False) 
                
                # --- 🎯 Second Step: Update required model fields 🎯 ---
                
                # Assign a unique primary key (project_id)
                new_project.project_id = str(uuid.uuid4())
                
                # Set the 'created_by' field (Assuming user authentication is enabled)
                # If authentication is NOT used, set a default value, e.g., 'SYSTEM'
                if request.user.is_authenticated:
                    new_project.created_by = request.user.username
                else:
                    new_project.created_by = 'ANONYMOUS' # Set a suitable default
                
                # The 'created_at' and 'updated_at' fields will be auto-set by the model
                
                # Save the object to the database
                new_project.save(using='snowflake_dev')
                
                # Success message and redirect
                messages.success(request, f"Project '{new_project.project_name}' successfully created!")
                return redirect('projects_dashboard') # Redirect to the dashboard URL name
            
            except Exception as e:
                # Handle database or other unexpected errors
                messages.error(request, f"Error saving project: {e}")
                # Continue to render the form with errors
        
        # If the form is NOT valid (e.g., project_name is missing)
        else:
            messages.error(request, "Please correct the errors below.")


    # 2. Handle GET Request (Initial Form Display)
    else:
        form = ProjectForm() # Create a fresh, unbound form

    # 3. Render the template (used for GET or POST with errors)
    context = {'form': form}
    return render(request, 'dq_management/project_form.html', context)


def edit_project(request, project_id):
    """
    Handles displaying the 'Edit Project' form (GET) and updating an existing project (POST).
    """
    print(f"--- DEBUG: edit_project called with project_id: {project_id} ---")
    project_data = get_project_from_db(project_id) # Assumes this returns a dict/object
    if not project_data:
        print(f"--- DEBUG: Project {project_id} not found ---")
        messages.error(request, "Project not found.")
        return redirect('projects_dashboard')

    # Map project_data dictionary keys to object attributes if necessary
    project = {
        'project_name': project_data.get('project_name') or project_data.project_name,
        'project_description': project_data.get('project_description') or project_data.project_description,
        'priority': project_data.get('priority') or project_data.priority,
        'created_by': project_data.get('created_by') or project_data.created_by,
    }
    print(f"--- DEBUG: Retrieved project data: {project} ---")


    if request.method == 'POST':
        print("--- DEBUG: POST received for edit_project ---")
        form = ProjectForm(request.POST)
        form.instance._state.db = 'snowflake_dev'  # Ensure form validation uses the correct database

        if form.is_valid():
            try:
                print("--- DEBUG: Form is valid, saving ---")
                # Get the unsaved instance
                updated_project = form.save(commit=False)
                updated_project.project_id = project_id  # Ensure the project_id is set
                updated_project.created_by = project['created_by']  # Retain original creator
                updated_project.save(using='snowflake_dev')

                print("--- DEBUG: Update SUCCESS. Redirecting to project_details. ---")
                messages.success(request, f"Project '{updated_project.project_name}' updated successfully! ✏️")
                logger.info(f"Project '{updated_project.project_name}' ({project_id}) updated.")
                return redirect(reverse('project_details', args=[project_id]))

            except Exception as e:
                print(f"--- DEBUG: UNCAUGHT EXCEPTION: {e} ---")
                messages.error(request, f"An unexpected error occurred during update: {e}")
        else:
            print("--- DEBUG: Form is invalid ---")
            messages.error(request, "Please correct the errors below.")

        # If form is invalid or exception occurred, re-render with errors
        context = {
            'form': form,
            'is_edit': True,
            'project_id': project_id,
            'title': f'Edit Project: {project["project_name"]}',
        }
        return render(request, 'dq_management/project_form.html', context)

    else:
        print("--- DEBUG: GET request for edit_project ---")
        form = ProjectForm(initial=project)
        context = {
            'form': form,
            'is_edit': True,
            'project_id': project_id,
            'title': f'Edit Project: {project["project_name"]}',
        }
        return render(request, 'dq_management/project_form.html', context)

def delete_project(request, project_id):
    """
    Handles the POST request to delete a project.
    Deletion is triggered by the dynamic form submission in JavaScript.
    """
    if request.method == 'POST':
        try:
            # ⭐ NOTE: You must implement this function in services.py
            # This function should also ensure all related test cases/groups are deleted.
            success, message = delete_project_from_db(project_id)
            
            if success:
                messages.success(request, message)
            else:
                messages.error(request, message)
                
        except Exception as e:
            messages.error(request, f"An unexpected error occurred during deletion: {e}")
            logger.error(f"Error deleting project {project_id}: {e}", exc_info=True)
            
        # Always redirect back to the main dashboard after deletion
        return redirect('projects_dashboard')
        
    messages.error(request, "Invalid request for project deletion.")
    return redirect('projects_dashboard')


def convert_numpy_types(obj):
    """
    Recursively converts numpy types within an object to standard Python types.
    This is used for JSON serialization.
    """
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(elem) for elem in obj]
    elif isinstance(obj, datetime): # Also handle datetime objects for consistency
        return obj.isoformat()
    return obj


def projects_dashboard(request):
    projects = get_all_projects_from_db()
    return render(request, 'dq_management/projects_dashboard.html', {'projects': projects})


def project_details(request, project_id):
    project = get_project_from_db(project_id)
    if not project:
        messages.error(request, "Project not found.")
        return redirect('projects_dashboard')
    test_cases = get_test_cases_for_project_from_db(project_id)
    test_groups = get_test_groups_for_project_from_db(project_id)
    
    context = {
        'project': project,
        'test_cases': test_cases,
        'test_groups': test_groups,
        'project_description': project.get('project_description'),
        'priority': project.get('priority'),
        'created_by': project.get('created_by'),
        'created_at': project.get('created_at'),
        'updated_at': project.get('updated_at'),
    }
    return render(request, 'dq_management/project_details.html', context)


def select_new_test_type(request, project_id):
    project = get_project_from_db(project_id)
    if not project:
        messages.error(request, "Project not found.")
        return redirect('projects_dashboard')
    return render(request, 'dq_management/select_test_type.html', {'project_id': project_id})


def manage_aggregation_test_form(request, project_id):
    project = get_project_from_db(project_id)
    if not project:
        messages.error(request, "Project not found.")
        return redirect('projects_dashboard')
    test_case_id = request.GET.get('test_case_id')
    test_case_data = None
    if test_case_id:
        test_case_data = get_test_case_details_from_db(test_case_id)
        if not test_case_data:
            messages.error(request, "Test case not found.")
            return redirect(reverse('project_details', args=[project_id]))
    available_connection_sources = get_available_connection_sources()
    context = {
        'project_id': project_id,
        'test_case': test_case_data,
        'test_case_id': test_case_id,
        'available_connection_sources': available_connection_sources,
        'now': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
    }
    return render(request, 'dq_management/aggregation.html', context)


def manage_drift_test_form(request, project_id):
    project = get_project_from_db(project_id)
    if not project:
        messages.error(request, "Project not found.")
        return redirect('projects_dashboard')
    test_case_id = request.GET.get('test_case_id')
    test_case_data = None
    if test_case_id:
        test_case_data = get_test_case_details_from_db(test_case_id)
        if not test_case_data:
            messages.error(request, "Test case not found.")
            return redirect(reverse('project_details', args=[project_id]))
    
    available_connection_sources = get_available_connection_sources()
    context = {
        'project_id': project_id,
        'test_case': test_case_data,
        'available_connection_sources': available_connection_sources
    }
    return render(request, 'dq_management/drift.html', context)


def manage_availability_test_form(request, project_id):
    project = get_project_from_db(project_id)
    if not project:
        messages.error(request, "Project not found.")
        return redirect('projects_dashboard')
    test_case_id = request.GET.get('test_case_id')
    test_case_data = None
    if test_case_id:
        test_case_data = get_test_case_details_from_db(test_case_id)
        if not test_case_data:
            messages.error(request, "Test case not found.")
            return redirect(reverse('project_details', args=[project_id]))
    
    available_connection_sources = get_available_connection_sources()
    context = {
        'project_id': project_id,
        'test_case': test_case_data,
        'available_connection_sources': available_connection_sources
    }
    return render(request, 'dq_management/availability.html', context)


def edit_test_case_dispatcher(request, test_case_id):
    test_case_data = get_test_case_details_from_db(test_case_id)
    if not test_case_data:
        messages.error(request, "Test case not found.")
        return redirect('projects_dashboard')
    test_type = test_case_data.get('test_type', '').strip().title()
    project_id = test_case_data.get('project_id')
    if test_type == 'Aggregation Comparison':
        return redirect(reverse('manage_aggregation_test_form', args=[project_id]) + f"?test_case_id={test_case_id}")
    elif test_type == 'Drift Test':
        return redirect(reverse('manage_drift_test_form', args=[project_id]) + f"?test_case_id={test_case_id}")
    elif test_type == 'Availability Test':
        return redirect(reverse('manage_availability_test_form', args=[project_id]) + f"?test_case_id={test_case_id}")
    else:
        messages.error(request, "Unknown test type. Cannot edit.")
        return redirect('projects_dashboard')


def handle_test_execution_or_save(request):
    if request.method == 'POST':
        form_data_raw = request.POST
        action = form_data_raw.get('action')
        project_id_from_form = form_data_raw.get('project_id')
        test_case_id_from_form = form_data_raw.get('test_case_id')
        
        test_type = form_data_raw.get('test_type', '').strip().title()
        config, validation_errors = _extract_and_validate_form_data(form_data_raw)
        config['test_type'] = test_type

        if action == 'save':
            if not validation_errors:
                new_test_case_id = test_case_id_from_form if test_case_id_from_form else str(uuid.uuid4())
                success = save_test_case_to_db(new_test_case_id, project_id_from_form, config)
                if success:
                    messages.success(request, f"Test case '{config['test_name']}' saved successfully!")
                    logger.info(f"Test case '{config['test_name']}' ({new_test_case_id}) saved to Snowflake.")
                else:
                    messages.error(request, f"Failed to save test case '{config['test_name']}'.")
                    logger.error(f"Failed to save test case '{config['test_name']}' ({new_test_case_id}) to Snowflake.")
                return redirect(reverse('project_details', args=[project_id_from_form]))
            else:
                for error in validation_errors:
                    messages.error(request, error)

        submitted_test_case_data = {
            'test_name': form_data_raw.get('test_name'),
            'source_connection_source': form_data_raw.get('source_connection_source'),
            'source_custom_sql': form_data_raw.get('source_custom_sql'),
            'source_table': form_data_raw.get('source_table'),
            'source_date_column': form_data_raw.get('source_date_column'),
            'source_date_value': form_data_raw.get('source_date_value'),
            'source_aggregation_type': form_data_raw.get('source_aggregation_type'),
            'source_aggregation_column': form_data_raw.get('source_aggregation_column'),
            'source_group_by_column': form_data_raw.get('source_group_by_column'),
            'source_additional_filters': form_data_raw.get('source_additional_filters'),
            'destination_connection_source': form_data_raw.get('destination_connection_source'),
            'destination_custom_sql': form_data_raw.get('destination_custom_sql'),
            'destination_table': form_data_raw.get('destination_table'),
            'destination_date_column': form_data_raw.get('destination_date_column'),
            'destination_date_value': form_data_raw.get('destination_date_value'),
            'destination_aggregation_type': form_data_raw.get('destination_aggregation_type'),
            'destination_aggregation_column': form_data_raw.get('destination_aggregation_column'),
            'destination_group_by_column': form_data_raw.get('destination_group_by_column'),
            'destination_additional_filters': form_data_raw.get('destination_additional_filters'),
            'threshold': form_data_raw.get('threshold'),
            'threshold_type': form_data_raw.get('threshold_type'),
            'historical_periods': form_data_raw.get('historical_periods'),
            'possible_resolution': form_data_raw.get('possible_resolution'),
        }

        preview_results = None
        test_results = None
        
        if not validation_errors and action != 'save':
            try:
                # TestCaseProcessor is initialized without db_credentials
                results_from_processor = TestCaseProcessor().process_test_request(config, action)
                if action == 'preview':
                    preview_results = results_from_processor
                elif action == 'run':
                    test_results = results_from_processor
            except Exception as e:
                logger.error(f"Error processing test request for action '{action}': {e}", exc_info=True)
                messages.error(request, f"An error occurred while running the test: {e}")
                validation_errors.append(f"An error occurred while running the test: {e}")

        available_connection_sources = get_available_connection_sources()
        
        render_params = {
            'test_case': submitted_test_case_data, 
            'validation_errors': validation_errors,
            'project_id': project_id_from_form,
            'test_case_id': test_case_id_from_form,
            'available_connection_sources': available_connection_sources,
            'preview_results': preview_results,
            'test_results': test_results,
        }

        template_map = {
            'Drift Test': 'dq_management/drift.html',
            'Aggregation Comparison': 'dq_management/aggregation.html',
            'Availability Test': 'dq_management/availability.html'
        }
        template_name = template_map.get(test_type)
        if template_name:
            render_params.update(submitted_test_case_data)
            return render(request, template_name, render_params)
        else:
            messages.error(request, "Unknown test type. Cannot display form.")
            return redirect('projects_dashboard')
            
    return redirect('projects_dashboard')


def run_adhoc_test(request, test_case_id):
    if request.method == 'POST':
        # Call the ORM-based run_adhoc_test_logic without db_credentials
        result = run_adhoc_test_logic(test_case_id)
        serializable_result = convert_numpy_types(result)
        return JsonResponse(serializable_result, status=serializable_result.get("status_code", 200))
    return JsonResponse({"error": "Invalid request method"}, status=405)

def delete_test_case(request, test_case_id):
    if request.method == 'POST':
        project_id = None
        try:
            # Call the ORM-based delete_test_case_from_db
            success, message, project_id = delete_test_case_from_db(test_case_id)
            if success:
                messages.success(request, message)
                logger.info(f"Test case {test_case_id} deleted successfully.")
            else:
                messages.error(request, message)
                logger.error(f"Failed to delete test case {test_case_id}. Error: {message}")
        except Exception as e:
            messages.error(request, f"An unexpected error occurred during deletion: {e}")
            logger.error(f"Unexpected error deleting test case {test_case_id}: {e}")
        if project_id:
            return redirect(reverse('project_details', args=[project_id]))
        return redirect('projects_dashboard')
    return JsonResponse({"error": "Invalid request method"}, status=405)

def create_test_group(request, project_id):
    project = get_project_from_db(project_id)
    if not project:
        messages.error(request, "Project not found.")
        return redirect('projects_dashboard')
    
    available_test_cases = get_test_cases_for_project_from_db(project_id)
    
    context = {
        'project': project,
        'available_test_cases': available_test_cases,
        'group_name_prefill': '',
        'group_description_prefill': '',
        'schedule_cron_prefill': '',
        'created_by_prefill': '',
        'selected_test_cases_prefill': [],
    }

    if request.method == 'POST':
        group_name = request.POST['group_name']
        group_description = request.POST.get('group_description')
        schedule_cron = request.POST.get('schedule_cron')
        created_by = request.POST.get('created_by')
        selected_test_cases_json = request.POST.get('selected_test_cases')
        selected_test_cases_data = json.loads(selected_test_cases_json) if selected_test_cases_json else []
        
        group_errors = []
        if not group_name.strip():
            group_errors.append("Test Group Name cannot be empty.")
        if not created_by or not created_by.strip():
            group_errors.append("Created By cannot be empty.")
        if not selected_test_cases_data:
            group_errors.append("At least one test case must be selected for the group.")
        
        if group_errors:
            for error in group_errors:
                messages.error(request, error)
            
            context.update({
                'group_name_prefill': group_name,
                'group_description_prefill': group_description,
                'schedule_cron_prefill': schedule_cron,
                'created_by_prefill': created_by,
                'selected_test_cases_prefill': [tc['test_case_id'] for tc in selected_test_cases_data],
            })
            return render(request, 'dq_management/create_test_group.html', context)
        
        new_group_id = str(uuid.uuid4())
        # Call the ORM-based save_test_group_to_db
        success = save_test_group_to_db(
            group_id=new_group_id,
            project_id=project_id,
            group_name=group_name,
            group_description=group_description,
            schedule_cron=schedule_cron,
            created_by=created_by,
            selected_test_cases_data=selected_test_cases_data
        )

        if success:
            messages.success(request, f"Test group '{group_name}' created successfully!")
            logger.info(f"Test group '{group_name}' ({new_group_id}) created by {created_by} saved to Snowflake.")
        else:
            messages.error(request, f"Failed to create test group '{group_name}'.")
            logger.error(f"Failed to create test group '{group_name}' ({new_group_id}) to Snowflake.")
        return redirect(reverse('project_details', args=[project_id]))
    
    return render(request, 'dq_management/create_test_group.html', context)

def run_test_group_async(request, group_id):
    if request.method == 'POST':
        logger.info(f"Received request to start async run for group: {group_id}")
        # Call the ORM-based start_group_run_task
        run_id = start_group_run_task(group_id)
        return JsonResponse({"run_id": run_id}, status=200)
    return JsonResponse({"error": "Invalid request method"}, status=405)

def get_run_status(request, run_id):
    status_data = get_group_run_status(run_id)
    serializable_status_data = convert_numpy_types(status_data)
    return JsonResponse(serializable_status_data)

def schedule_test_group(request, group_id):
    if request.method == 'POST':
        # Call the ORM-based schedule_test_group_logic
        result = schedule_test_group_logic(group_id)
        serializable_result = convert_numpy_types(result)
        return JsonResponse(serializable_result)
    return JsonResponse({"error": "Invalid request method"}, status=405)

def edit_test_group(request, group_id):
    group_data = get_test_group_details_from_db(group_id)
    if not group_data:
        messages.error(request, "Test group not found.")
        return redirect('projects_dashboard')

    project_id = group_data.get('project_id')
    project = get_project_from_db(project_id)
    if not project:
        messages.error(request, "Associated project not found.")
        return redirect('projects_dashboard')

    available_test_cases = get_test_cases_for_project_from_db(project_id)
    selected_test_cases_data = get_test_cases_in_group_from_db(group_id)
    selected_test_case_ids = [tc['id'] for tc in selected_test_cases_data]
    
    context = {
        'project': project,
        'available_test_cases': available_test_cases,
        'group_id': group_id,
        'group_name_prefill': group_data.get('group_name'),
        'group_description_prefill': group_data.get('group_description'),
        'schedule_cron_prefill': group_data.get('schedule_cron'),
        'created_by_prefill': group_data.get('created_by'),
        'selected_test_cases_prefill': selected_test_case_ids,
        'group_data': group_data,
        'selected_test_cases_data': json.dumps(_to_json_safe(selected_test_cases_data)),  # Use the helper
    }

    if request.method == 'POST':
        group_name = request.POST['group_name']
        group_description = request.POST.get('group_description')
        schedule_cron = request.POST.get('schedule_cron')
        created_by = request.POST.get('created_by')
        
        selected_test_cases_json = request.POST.get('selected_test_cases')
        new_selected_test_cases = json.loads(selected_test_cases_json) if selected_test_cases_json else []

        group_errors = []
        if not group_name.strip():
            group_errors.append("Test Group Name cannot be empty.")
        if not created_by or not created_by.strip():
            group_errors.append("Created By cannot be empty.")
        if not new_selected_test_cases:
            group_errors.append("At least one test case must be selected for the group.")
        
        if group_errors:
            for error in group_errors:
                messages.error(request, error)
            
            context.update({
                'group_name_prefill': group_name,
                'group_description_prefill': group_description,
                'schedule_cron_prefill': schedule_cron,
                'created_by_prefill': created_by,
                'selected_test_cases_prefill': [tc['test_case_id'] for tc in new_selected_test_cases],
            })
            return render(request, 'dq_management/create_test_group.html', context)
        
        # Call the ORM-based save_test_group_to_db
        success = save_test_group_to_db(
            group_id=group_id,
            project_id=project_id,
            group_name=group_name,
            group_description=group_description,
            schedule_cron=schedule_cron,
            created_by=created_by,
            selected_test_cases_data=new_selected_test_cases
        )

        if success:
            messages.success(request, f"Test group '{group_name}' updated successfully!")
            logger.info(f"Test group '{group_name}' ({group_id}) updated in Snowflake.")
        else:
            messages.error(request, f"Failed to update test group '{group_name}'.")
            logger.error(f"Failed to update test group '{group_name}' ({group_id}) to Snowflake.")

        return redirect(reverse('project_details', args=[project_id]))

    return render(request, 'dq_management/create_test_group.html', context)

def delete_test_group(request, group_id):
    if request.method == 'POST':
        # Call the ORM-based delete_test_group_from_db
        success, message, project_id = delete_test_group_from_db(group_id)
        
        if success:
            messages.success(request, message)
            logger.info(f"Test group {group_id} deleted successfully.")
        else:
            messages.error(request, message)
            logger.error(f"Failed to delete test group {group_id}. Error: {message}")

        if project_id:
            return redirect(reverse('project_details', args=[project_id]))
        return redirect('projects_dashboard')
    return JsonResponse({"error": "Invalid request method"}, status=405)

def test_case_logs(request):
    logs = get_test_case_logs_from_db()
    return render(request, 'dq_management/test_case_logs.html', {'logs': logs})

def test_group_logs(request):
    logs = get_test_group_logs_from_db()
    return render(request, 'dq_management/test_group_logs.html', {'logs': logs})

def logs(request):
    test_case_logs = get_test_case_logs_from_db()
    test_group_logs = get_test_group_logs_from_db()
    context = {
        'test_case_logs': test_case_logs,
        'test_group_logs': test_group_logs,
    }
    return render(request, 'dq_management/logs.html', context)

def dashboard(request):
    """
    Render dashboard shell. Charts now fetch data dynamically from an API endpoint.
    """
    from collections import defaultdict

    projects = get_all_projects_from_db()[:50]  # Limit to 50 projects for performance

    # Fetch all logs at once to avoid N queries
    all_logs = list(TestGroupLog.objects.using('snowflake_dev').values('project_id', 'test_group_id', 'status', 'start_timestamp'))
    logs_by_project = defaultdict(list)
    for log in all_logs:
        logs_by_project[log['project_id']].append(log)

    # Compute per-project status summary based on latest TestGroupLog per group
    all_projects_summarized = []
    for project in projects:
        project_id = project.get('project_id') or project.get('id')
        logs = logs_by_project.get(project_id, [])
        latest_by_group = {}
        for row in logs:
            gid = row['test_group_id']
            ts = row['start_timestamp']
            if gid is None:
                continue
            if gid not in latest_by_group or (ts and latest_by_group[gid]['start_timestamp'] and ts > latest_by_group[gid]['start_timestamp']) or (latest_by_group[gid]['start_timestamp'] is None and ts is not None):
                latest_by_group[gid] = row

        total_run_groups = 0
        passed_test_groups = 0
        failed_test_groups = 0
        for row in latest_by_group.values():
            status = (row.get('status') or '').upper()
            if status in ('PASS', 'FAIL', 'ERROR'):
                total_run_groups += 1
            if status == 'PASS':
                passed_test_groups += 1
            elif status in ('FAIL', 'ERROR'):
                failed_test_groups += 1

        # Determine overall project status
        if total_run_groups == 0:
            overall_status = 'NORUN'
        elif failed_test_groups > 0:
            overall_status = 'FAILED'
        else:
            overall_status = 'PASSED'

        # Get criticality from project data
        criticality = project.get('criticality_level') or project.get('priority', 'Low')
        summary = {
            'project_id': project_id,
            'project_name': project.get('project_name') or project.get('name'),
            'project_description': project.get('project_description',''),
            'priority': criticality,  # Use criticality_level instead of priority
            'failed_test_groups': failed_test_groups,
            'passed_test_groups': passed_test_groups,
            'total_run_groups': total_run_groups,
            'overall_status': overall_status,
        }
        all_projects_summarized.append(summary)

    # Recent logs for table
    test_group_logs = get_test_group_logs_from_db()[:50]

    context = {
        'all_projects_summarized': all_projects_summarized,
        'test_group_logs': test_group_logs,
    }
    return render(request, 'dq_management/dashboard.html', context)


def dashboard_summary_api(request):
    """Return real-time summary for charts based on latest TestGroupLog per group."""
    priorities = ['Critical', 'High', 'Medium', 'Low']
    data = {'failed': {p: 0 for p in priorities}, 'passed': {p: 0 for p in priorities}}

    # Get latest log per test_group_id
    try:
        # Use ORM to fetch and then reduce to latest per group in Python to avoid DB-specific SQL
        logs = list(TestGroupLog.objects.using('snowflake_dev').all().values('test_group_id', 'status', 'criticality', 'start_timestamp'))
        latest_by_group = {}
        for row in logs:
            gid = row['test_group_id']
            ts = row['start_timestamp']
            if gid is None:
                # Skip malformed rows
                continue
            if gid not in latest_by_group or (ts and latest_by_group[gid]['start_timestamp'] and ts > latest_by_group[gid]['start_timestamp']) or (latest_by_group[gid]['start_timestamp'] is None and ts is not None):
                latest_by_group[gid] = row

        for row in latest_by_group.values():
            status = (row.get('status') or '').upper()
            crit = row.get('criticality') or 'Low'
            if crit not in data['failed']:
                crit = 'Low'
            if status in ('FAIL', 'ERROR'):
                data['failed'][crit] += 1
            elif status == 'PASS':
                data['passed'][crit] += 1
            else:
                # Unknown or RUNNING -> do not count
                pass

        totals = {
            'failed': sum(data['failed'].values()),
            'passed': sum(data['passed'].values()),
        }
        return JsonResponse({'failed': data['failed'], 'passed': data['passed'], 'totals': totals})
    except Exception as e:
        logger.exception('dashboard_summary_api error: %s', e)
        return JsonResponse({'failed': {p:0 for p in priorities}, 'passed': {p:0 for p in priorities}, 'totals': {'failed':0,'passed':0}}, status=200)


def create_or_edit_test_group(request, project_id, group_id=None):
    project = get_object_or_404(Project, project_id=project_id)
    available_test_cases = TestCase.objects.using('snowflake_dev').filter(project_id=project_id).order_by('test_name')

    # --- Prefill logic for selected test cases and execution order ---
    selected_test_cases_data = []
    if group_id:
        # Editing: fetch selected test cases and their order
        junction_records = TestGroupTestCase.objects.using('snowflake_dev').filter(test_group_id=group_id).order_by('execution_order').select_related('test_case')
        for record in junction_records:
            tc = record.test_case
            tc_data = {
                "detail": {
                    "test_case_id": tc.test_case_id,
                    "test_name": tc.test_name,
                    "test_type": tc.test_type,
                    "source_table": tc.source_table,
                    "destination_table": tc.destination_table,
                    "source_agg_type": tc.source_agg_type,
                    "source_agg_column": tc.source_agg_column,
                    "destination_agg_type": tc.destination_agg_type,
                    "destination_agg_column": tc.destination_agg_column,
                    "created_by": tc.created_by,
                    "updated_at": tc.updated_at.strftime('%Y-%m-%d %H:%M:%S') if tc.updated_at else '',
                },
                "execution_order": record.execution_order
            }
            selected_test_cases_data.append(tc_data)
    # For create, selected_test_cases_data remains empty

    context = {
        "project": project,
        "available_test_cases": available_test_cases,
        "group_id": group_id,
        "group_name_prefill": '',
        "group_description_prefill": '',
        "schedule_cron_prefill": '',
        "created_by_prefill": '',
        "selected_test_cases_prefill": [],
        "selected_test_cases_data": json.dumps(_to_json_safe(selected_test_cases_data)),
    }

    if request.method == 'POST':
        group_name = request.POST['group_name']
        group_description = request.POST.get('group_description')
        schedule_cron = request.POST.get('schedule_cron')
        created_by = request.POST.get('created_by')

        selected_test_cases_json = request.POST.get('selected_test_cases')
        new_selected_test_cases = json.loads(selected_test_cases_json) if selected_test_cases_json else []

        group_errors = []
        if not group_name.strip():
            group_errors.append("Test Group Name cannot be empty.")
        if not created_by or not created_by.strip():
            group_errors.append("Created By cannot be empty.")
        if not new_selected_test_cases:
            group_errors.append("At least one test case must be selected for the group.")

        if group_errors:
            for error in group_errors:
                messages.error(request, error)

            context.update({
                'group_name_prefill': group_name,
                'group_description_prefill': group_description,
                'schedule_cron_prefill': schedule_cron,
                'created_by_prefill': created_by,
                'selected_test_cases_prefill': [tc['test_case_id'] for tc in new_selected_test_cases],
            })
            return render(request, 'dq_management/create_test_group.html', context)

        if group_id:
            # Editing an existing group
            success = save_test_group_to_db(
                group_id=group_id,
                project_id=project_id,
                group_name=group_name,
                group_description=group_description,
                schedule_cron=schedule_cron,
                created_by=created_by,
                selected_test_cases_data=new_selected_test_cases
            )
            if success:
                messages.success(request, f"Test group '{group_name}' updated successfully!")
                logger.info(f"Test group '{group_name}' ({group_id}) updated in Snowflake.")
            else:
                messages.error(request, f"Failed to update test group '{group_name}'.")
                logger.error(f"Failed to update test group '{group_name}' ({group_id}) to Snowflake.")
        else:
            # Creating a new group
            new_group_id = str(uuid.uuid4())
            success = save_test_group_to_db(
                group_id=new_group_id,
                project_id=project_id,
                group_name=group_name,
                group_description=group_description,
                schedule_cron=schedule_cron,
                created_by=created_by,
                selected_test_cases_data=new_selected_test_cases
            )
            if success:
                messages.success(request, f"Test group '{group_name}' created successfully!")
                logger.info(f"Test group '{group_name}' ({new_group_id}) created by {created_by} saved to Snowflake.")
            else:
                messages.error(request, f"Failed to create test group '{group_name}'.")
                logger.error(f"Failed to create test group '{group_name}' ({new_group_id}) to Snowflake.")

        return redirect(reverse('project_details', args=[project_id]))

    return render(request, 'dq_management/create_test_group.html', context)


def flow_view(request, project_id):
    """
    Renders a visual flow diagram of test groups and test cases for a project.
    Shows execution order, status, and relationships.
    """
    project = get_project_from_db(project_id)
    if not project:
        messages.error(request, "Project not found.")
        return redirect('dashboard')

    # Get test groups for the project
    test_groups = get_test_groups_for_project_from_db(project_id)

    # Get test cases for the project
    test_cases = get_test_cases_for_project_from_db(project_id)

    # Get recent logs for status information
    test_group_logs = get_test_group_logs_from_db()
    test_case_logs = get_test_case_logs_from_db()

    # Create a mapping of test group to its test cases with execution order
    group_to_cases = {}
    for group in test_groups:
        group_id = group.get('test_group_id')
        cases_in_group = get_test_cases_in_group_from_db(group_id)
        # Sort by execution order if available
        cases_in_group.sort(key=lambda x: x.get('execution_order', 999))
        group_to_cases[group_id] = cases_in_group

    # Get latest status for each test group and test case
    group_statuses = {}
    case_statuses = {}

    for log in test_group_logs:
        if log.get('test_group_id') not in group_statuses:
            group_statuses[log.get('test_group_id')] = log.get('status', 'UNKNOWN')

    for log in test_case_logs:
        if log.get('test_case_id') not in case_statuses:
            case_statuses[log.get('test_case_id')] = log.get('run_status', 'UNKNOWN')

    # Generate Graphviz diagram
    dot = Digraph(comment='Test Flow Diagram', format='svg')
    dot.attr(rankdir='TD', size='10,10')

    # Color mapping for statuses
    status_colors = {
        'PASS': 'lightgreen',
        'FAIL': 'lightcoral',
        'ERROR': 'orange',
        'RUNNING': 'lightblue',
        'UNKNOWN': 'lightgray'
    }

    # Add test groups as main nodes
    for group in test_groups:
        group_id = group.get('test_group_id')
        if not group_id:
            continue
        status = group_statuses.get(group_id, 'UNKNOWN')
        color = status_colors.get(status, 'lightgray')
        group_name = group.get('group_name') or 'Unknown Group'
        dot.node(group_id, f"{group_name}\n(Group)", style='filled', fillcolor=color)

    # Add test cases and connections
    for group in test_groups:
        group_id = group.get('test_group_id')
        if not group_id:
            continue
        cases = group_to_cases.get(group_id, [])
        prev_case_id = None
        for case in cases:
            case_id = case.get('test_case_id')
            if not case_id:
                continue
            status = case_statuses.get(case_id, 'UNKNOWN')
            color = status_colors.get(status, 'lightgray')
            test_name = case.get('test_name') or 'Unknown Test'
            test_type = case.get('test_type') or 'Unknown Type'
            dot.node(case_id, f"{test_name}\n({test_type})", style='filled', fillcolor=color)
            dot.edge(group_id, case_id)
            if prev_case_id:
                dot.edge(prev_case_id, case_id)
            prev_case_id = case_id

    # Add standalone test cases (not in any group)
    for case in test_cases:
        case_id = case.get('test_case_id')
        if not case_id:
            continue
        is_in_group = any(case_id in [c.get('test_case_id') for c in group_to_cases.get(gid, []) if c.get('test_case_id')] for gid in group_to_cases)
        if not is_in_group:
            status = case_statuses.get(case_id, 'UNKNOWN')
            color = status_colors.get(status, 'lightgray')
            test_name = case.get('test_name') or 'Unknown Test'
            test_type = case.get('test_type') or 'Unknown Type'
            dot.node(case_id, f"{test_name}\n({test_type})", style='filled', fillcolor=color)

    # Render the diagram to SVG string
    svg_content = dot.pipe().decode('utf-8')

    context = {
        'project': project,
        'test_groups': test_groups,
        'test_cases': test_cases,
        'group_to_cases': group_to_cases,
        'group_statuses': group_statuses,
        'case_statuses': case_statuses,
        'svg_content': svg_content,
    }

    return render(request, 'dq_management/flow.html', context)


def welcome(request):
    """
    Returns a welcome message and logs the request method and path.
    """
    logger.info(f"Request received: {request.method} {request.path}")
    return JsonResponse({"message": "Welcome to the Django API Service!"})


def test_group_flow(request, group_id):
    """
    Renders a visual flow diagram of test cases within a test group.
    Shows execution order, status, and possible resolutions for failed tests.
    """
    group_data = get_test_group_details_from_db(group_id)
    if not group_data:
        messages.error(request, "Test group not found.")
        return redirect('dashboard')

    project_id = group_data.get('project_id')
    project = get_project_from_db(project_id)
    if not project:
        messages.error(request, "Associated project not found.")
        return redirect('dashboard')

    # Get test cases in the group with execution order
    test_cases = get_test_cases_in_group_from_db(group_id)
    test_cases.sort(key=lambda x: x.get('execution_order', 999))

    # Get recent logs for status information
    test_group_logs = get_test_group_logs_from_db()
    test_case_logs = get_test_case_logs_from_db()

    # Get latest status for the test group
    group_status = 'UNKNOWN'
    for log in test_group_logs:
        if log.get('test_group_id') == group_id:
            group_status = log.get('status', 'UNKNOWN')
            break

    # Get latest status for each test case
    case_statuses = {}
    case_details = {}
    for log in test_case_logs:
        tc_id = log.get('test_case_id')
        if tc_id and tc_id not in case_statuses:
            case_statuses[tc_id] = log.get('run_status', 'UNKNOWN')
            case_details[tc_id] = {
                'message': log.get('message', ''),
                'possible_resolution': log.get('possible_resolution', ''),
                'run_message': log.get('run_message', ''),
                'source_query': log.get('source_query', ''),
                'destination_query': log.get('destination_query', ''),
            }

    # Generate Graphviz diagram for single group
    dot = Digraph(comment='Test Group Flow Diagram', format='svg')
    dot.attr(rankdir='TD', size='10,10')

    # Color mapping for statuses
    status_colors = {
        'PASS': 'lightgreen',
        'FAIL': 'lightcoral',
        'ERROR': 'orange',
        'RUNNING': 'lightblue',
        'UNKNOWN': 'lightgray'
    }

    # Add the test group as main node
    group_id_str = group_data.get('test_group_id')
    if not group_id_str:
        group_id_str = 'unknown_group'
    group_name = group_data.get('group_name') or 'Unknown Group'
    color = status_colors.get(group_status, 'lightgray')
    dot.node(group_id_str, f"{group_name}\n(Group)", style='filled', fillcolor=color)

    # Add test cases and connections
    prev_case_id = None
    for case in test_cases:
        case_id = case.get('test_case_id')
        if not case_id:
            continue
        status = case_statuses.get(case_id, 'UNKNOWN')
        color = status_colors.get(status, 'lightgray')
        test_name = case.get('test_name') or 'Unknown Test'
        test_type = case.get('test_type') or 'Unknown Type'
        dot.node(case_id, f"{test_name}\n({test_type})", style='filled', fillcolor=color)
        dot.edge(group_id_str, case_id)
        if prev_case_id:
            dot.edge(prev_case_id, case_id)
        prev_case_id = case_id

    # Render the diagram to SVG string
    svg_content = dot.pipe().decode('utf-8')

    # Prepare data for template
    test_groups = [group_data]  # Single group
    group_to_cases = {group_id: test_cases}
    group_statuses = {group_id: group_status}
    case_statuses_for_template = case_statuses

    context = {
        'project': project,
        'group': group_data,
        'test_groups': test_groups,
        'test_cases': test_cases,
        'group_to_cases': group_to_cases,
        'group_statuses': group_statuses,
        'case_statuses': case_statuses_for_template,
        'case_details': case_details,
        'svg_content': svg_content,
    }

    return render(request, 'dq_management/flow.html', context)

