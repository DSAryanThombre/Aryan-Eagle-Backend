from django.db import models

# --- 1. PROJECTS Table ---
class Project(models.Model):
    CRITICALITY_LEVEL_CHOICES = [
        ('Critical', 'Critical'),
        ('High', 'High'),
        ('Medium', 'Medium'),
        ('Low', 'Low'),
        ('Occasional', 'Occasional'),
    ]

    project_id = models.CharField(max_length=36, primary_key=True)
    project_name = models.CharField(max_length=255)
    project_description = models.TextField(max_length=1000, blank=True, null=True)
    created_by = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    priority = models.CharField(max_length=50, blank=True, null=True)
    criticality_level = models.CharField(max_length=20, choices=CRITICALITY_LEVEL_CHOICES, blank=True, null=True)
    

    class Meta:
        managed = False
        db_table = 'PROJECTS'
        verbose_name = 'Project'
        verbose_name_plural = 'Projects'

    def __str__(self):
        return self.project_name

# --- 2. TEST_CASES Table ---
class TestCase(models.Model):
    CATEGORY_CHOICES = [
        ('POWER BI', 'POWER BI'),
        ('SQL', 'SQL'),
        ('SAP', 'SAP'),
        ('TBH', 'TBH'),
    ]

    test_case_id = models.CharField(max_length=36, primary_key=True)
    project = models.ForeignKey(Project, on_delete=models.CASCADE, db_column='PROJECT_ID')
    category = models.CharField(max_length=25, blank=True, null=True, choices=CATEGORY_CHOICES)
    test_name = models.CharField(max_length=255)
    test_type = models.CharField(max_length=50, blank=True, null=True)
    source_connection_source = models.CharField(max_length=255)
    destination_connection_source = models.CharField(max_length=255)
    source_table = models.CharField(max_length=255, blank=True, null=True)
    destination_table = models.CharField(max_length=255, blank=True, null=True)
    additional_source_filters = models.CharField(max_length=255, blank=True, null=True)
    additional_destination_filters = models.CharField(max_length=255, blank=True, null=True)
    custom_source_sql = models.TextField(max_length=4000, blank=True, null=True)
    custom_destination_sql = models.TextField(max_length=4000, blank=True, null=True)
    threshold = models.DecimalField(max_digits=18, decimal_places=4, blank=True, null=True)
    threshold_type = models.CharField(max_length=50)
    status = models.CharField(max_length=50, default='DRAFT')
    created_by = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    source_date_column = models.CharField(max_length=255, blank=True, null=True)
    source_agg_type = models.CharField(max_length=50, blank=True, null=True)
    source_agg_column = models.CharField(max_length=255, blank=True, null=True)
    source_group_by_column = models.CharField(max_length=255, blank=True, null=True)
    destination_date_column = models.CharField(max_length=255, blank=True, null=True)
    destination_agg_type = models.CharField(max_length=50, blank=True, null=True)
    destination_agg_column = models.CharField(max_length=255, blank=True, null=True)
    destination_group_by_column = models.CharField(max_length=255, blank=True, null=True)
    source_date_value = models.CharField(max_length=255, blank=True, null=True)
    destination_date_value = models.CharField(max_length=255, blank=True, null=True)
    possible_resolution = models.TextField(max_length=4000, blank=True, null=True)
    # --- Power BI specific fields (NEW SCHEMA) ---
    destination_workspace_id = models.CharField(max_length=255, blank=True, null=True, db_column='DESTINATION_WORKSPACE_ID')
    destination_dataset_id = models.CharField(max_length=255, blank=True, null=True, db_column='DESTINATION_DATASET_ID')
    destination_dax_query = models.CharField(max_length=255, blank=True, null=True, db_column='DESTINATION_DAX_QUERY')
    source_workspace_id = models.CharField(max_length=255, blank=True, null=True, db_column='SOURCE_WORKSPACE_ID')
    source_dataset_id = models.CharField(max_length=255, blank=True, null=True, db_column='SOURCE_DATASET_ID')
    source_dax_query = models.TextField(max_length=4000, blank=True, null=True, db_column='SOURCE_DAX_QUERY')

    class Meta:
        managed = False
        db_table = 'TEST_CASES'
        verbose_name = 'Test Case'
        verbose_name_plural = 'Test Cases'

    def __str__(self):
        return self.test_name

# --- 3. TEST_GROUPS Table ---
class TestGroup(models.Model):
    test_group_id = models.CharField(max_length=36, primary_key=True)
    project = models.ForeignKey(Project, on_delete=models.CASCADE, db_column='PROJECT_ID')
    group_name = models.CharField(max_length=255)
    group_description = models.TextField(max_length=1000, blank=True, null=True)
    schedule_cron = models.CharField(max_length=255, blank=True, null=True)
    status = models.CharField(max_length=50, default='ACTIVE')
    last_run = models.DateTimeField(blank=True, null=True)
    failed_tests = models.IntegerField(blank=True, null=True)
    total_tests = models.IntegerField(blank=True, null=True)
    created_by = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    test_cases = models.ManyToManyField(
        TestCase,
        through='TestGroupTestCase',
        related_name='test_groups'
    )

    class Meta:
        managed = False
        db_table = 'TEST_GROUPS'
        verbose_name = 'Test Group'
        verbose_name_plural = 'Test Groups'

    def __str__(self):
        return self.group_name

# --- 4. TEST_GROUP_TEST_CASES Table (Junction Table for Many-to-Many) ---
class TestGroupTestCase(models.Model):
    test_group = models.ForeignKey(TestGroup, on_delete=models.CASCADE, db_column='TEST_GROUP_ID', primary_key=True)
    test_case = models.ForeignKey(TestCase, on_delete=models.CASCADE, db_column='TEST_CASE_ID')
    execution_order = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'TEST_GROUP_TEST_CASES'
        unique_together = (('test_group', 'test_case'),)
        verbose_name = 'Test Group Test Case'
        verbose_name_plural = 'Test Group Test Cases'

    def __str__(self):
        return f"{self.test_group.group_name} - {self.test_case.test_name} (Order: {self.execution_order})"

# --- 5. TEST_GROUP_LOGS Table ---
class TestGroupLog(models.Model):
    run_id = models.CharField(max_length=255, primary_key=True)
    # Changed from ForeignKey to CharField
    test_group_id = models.CharField(max_length=36, blank=True, null=True, db_column='TEST_GROUP_ID')
    group_name = models.CharField(max_length=255, blank=True, null=True) # Snapshot of group name
    # Changed from ForeignKey to CharField
    project_id = models.CharField(max_length=36, blank=True, null=True, db_column='PROJECT_ID') # Snapshot of project ID
    project_name = models.CharField(max_length=255, blank=True, null=True) # Snapshot of project name
    criticality = models.CharField(max_length=50, blank=True, null=True) # Snapshot of project criticality
    start_timestamp = models.DateTimeField(blank=True, null=True)
    end_timestamp = models.DateTimeField(blank=True, null=True)
    status = models.CharField(max_length=50)
    message = models.TextField(max_length=1000, blank=True, null=True)
    results_details = models.JSONField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'TEST_GROUP_LOGS'
        verbose_name = 'Test Group Log'
        verbose_name_plural = 'Test Group Logs'

    def __str__(self):
        return f"Run {self.run_id} - {self.group_name or 'N/A'} - {self.status}"

# --- 6. TEST_CASE_LOGS Table ---
class TestCaseLog(models.Model):
    run_id = models.CharField(max_length=255, primary_key=True)
    # Changed from ForeignKey to CharField
    test_case_id = models.CharField(max_length=36, blank=True, null=True, db_column='TEST_CASE_ID')
    # Changed from ForeignKey to CharField
    project_id = models.CharField(max_length=36, blank=True, null=True, db_column='PROJECT_ID')
    test_name = models.CharField(max_length=255, blank=True, null=True) # Snapshot of test name
    test_type = models.CharField(max_length=255, blank=True, null=True) # Snapshot of test type
    project_name = models.CharField(max_length=255, blank=True, null=True) # Snapshot of project name
    criticality = models.CharField(max_length=50, blank=True, null=True) # Snapshot of project criticality
    run_status = models.CharField(max_length=50, blank=True, null=True)
    run_message = models.TextField(blank=True, null=True)
    source_value = models.JSONField(blank=True, null=True)
    destination_value = models.JSONField(blank=True, null=True)
    difference = models.JSONField(blank=True, null=True)
    threshold_type = models.CharField(max_length=16777216, blank=True, null=True)  # Changed
    threshold_value = models.CharField(max_length=16777216, blank=True, null=True) # Changed
    source_query = models.TextField(blank=True, null=True)
    destination_query = models.TextField(blank=True, null=True)
    run_timestamp = models.DateTimeField(auto_now_add=True)
    source_connection_used = models.CharField(max_length=255, blank=True, null=True)
    destination_connection_used = models.CharField(max_length=255, blank=True, null=True)
    parent_run_id = models.CharField(max_length=255, blank=True, null=True)
    possible_resolution = models.TextField(max_length=4000, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'TEST_CASE_LOGS'
        verbose_name = 'Test Case Log'
        verbose_name_plural = 'Test Case Logs'

    def __str__(self):
        return f"Log {self.run_id} - {self.test_name or 'N/A'} - {self.run_status or 'N/A'}"

        