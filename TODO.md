# TODO: Implement View Flow Modal on Dashboard

## 1. Backend Changes
- [x] Add new API endpoint in urls.py: `path('api/test_groups/<str:group_id>/flow_data/', views.test_group_flow_data_api, name='test_group_flow_data_api')`
- [x] Implement `test_group_flow_data_api` view in views.py:
  - Load group details using `get_test_group_details_from_db`
  - Load ordered test cases using `get_test_cases_in_group_from_db`
  - Get latest TestGroupLog for the group
  - Build dict of latest TestCaseLog per test case
  - Return JSON response with group and tests data

## 2. Frontend Changes
- [x] Update dashboard.html:
  - Replace "View Flow" link in "Recent Alerts & Logs" with a button that triggers modal
  - Add modal HTML structure for "Group Flow"
  - Add JavaScript for:
    - Click handler to fetch flow data
    - Render simple HTML flow with boxes, arrows, and status colors
    - Show/hide modal with close handlers

## 3. Testing
- [ ] Verify API endpoint returns correct JSON structure
- [ ] Test modal opens and renders flow correctly
- [ ] Check status coloring (PASS: green, FAIL/ERROR: red, RUNNING: blue, UNKNOWN: gray)
