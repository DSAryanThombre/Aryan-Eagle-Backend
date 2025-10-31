# dq_management/urls.py

from django.urls import path
from . import views

urlpatterns = [
    # Displays a dashboard with cards for each project
    path('', views.projects_dashboard, name='projects_dashboard'),

    # URL pattern to handle creating a new project
    path('projects/new/', views.create_project, name='create_project'),
    
    # URL pattern to handle editing an existing project
    path('projects/<str:project_id>/edit/', views.edit_project, name='edit_project'), 
    
    # ⭐ FIX: Add URL pattern for project deletion
    path('projects/<str:project_id>/delete/', views.delete_project, name='delete_project'), 
    
    # Displays details for a single project
    path('projects/<str:project_id>/', views.project_details, name='project_details'),
    
    # Lets the user choose the type of new test case to create
    path('projects/<str:project_id>/test_cases/new/', views.select_new_test_type, name='select_new_test_type'),

    # Renders the form for a new aggregation test
    path('projects/<str:project_id>/test_cases/new/aggregation/', views.manage_aggregation_test_form, name='manage_aggregation_test_form'),

    # Renders the form for a new drift test
    path('projects/<str:project_id>/test_cases/new/drift/', views.manage_drift_test_form, name='manage_drift_test_form'),

    # Renders the form for a new availability test
    path('projects/<str:project_id>/test_cases/new/availability/', views.manage_availability_test_form, name='manage_availability_test_form'),

    # Dispatches to the correct edit form based on test type
    path('test_cases/<str:test_case_id>/edit/', views.edit_test_case_dispatcher, name='edit_test_case_dispatcher'),
    
    # Handles saving a test definition, or running/previewing a test
    path('handle_test_execution_or_save/', views.handle_test_execution_or_save, name='handle_test_execution_or_save'),
    
    # API endpoint to run a test case ad-hoc
    path('run_adhoc_test/<str:test_case_id>/', views.run_adhoc_test, name='run_adhoc_test'),
    
    # Deletes a test case
    path('delete_test_case/<str:test_case_id>/', views.delete_test_case, name='delete_test_case'),
    
    # Creates and edits test groups
    path('projects/<str:project_id>/test_groups/new/', views.create_test_group, name='create_test_group'),
    path('test_groups/<str:group_id>/edit/', views.edit_test_group, name='edit_test_group'),
    
    # Deletes a test group
    path('delete_test_group/<str:group_id>/', views.delete_test_group, name='delete_test_group'),
    
    # API endpoints for asynchronous test group runs
    path('test_groups/<str:group_id>/run_async/', views.run_test_group_async, name='run_test_group_async'),
    path('test_groups/run_status/<str:run_id>/', views.get_run_status, name='get_run_status'),
    path('test_groups/<str:group_id>/schedule/', views.schedule_test_group, name='schedule_test_group'),

    # API endpoints for asynchronous project runs
    path('projects/<str:project_id>/run_async/', views.run_project_async, name='run_project_async'),
    path('projects/run_status/<str:run_id>/', views.get_project_run_status, name='get_project_run_status'),
    
    # Log pages
    path('test_case_logs/', views.test_case_logs, name='test_case_logs'),
    path('test_group_logs/', views.test_group_logs, name='test_group_logs'),
    path('logs/', views.logs, name='logs'),

    # Dashboard
    path('dashboard/', views.dashboard, name='dashboard'),
    path('api/dashboard/summary/', views.dashboard_summary_api, name='dashboard_summary_api'),

    # Welcome endpoint
    path('welcome/', views.welcome, name='welcome'),

    # Flow view for project visualization
    path('flow/<str:project_id>/', views.flow_view, name='flow_view'),

    # Flow view for test group visualization
    path('test_groups/<str:group_id>/flow/', views.test_group_flow, name='test_group_flow'),

    # API endpoint for test group flow data
    path('api/test_groups/<str:group_id>/flow_data/', views.test_group_flow_data_api, name='test_group_flow_data_api'),
]
