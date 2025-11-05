# dq_management/forms.py

from django import forms
# You MUST ensure your 'Project' model is importable from where this forms.py file is.
# Update this import path if your models.py is in a different location (e.g., .models)
from .models import Project  

class ProjectForm(forms.ModelForm):
    """
    Form for creating a new data quality project.
    Applies custom CSS classes for Tailwind styling defined in the template.
    """
    criticality_level = forms.ChoiceField(
        choices=Project.CRITICALITY_LEVEL_CHOICES,
        required=True,
        widget=forms.Select(attrs={
            'class': 'form-select',
        }),
    )

    class Meta:
        model = Project
        fields = ['project_name', 'project_description', 'criticality_level']

        # Define widgets to apply custom HTML attributes and CSS classes
        widgets = {
            'project_name': forms.TextInput(attrs={
                'class': 'form-input',
                'placeholder': 'Enter a unique project name (e.g., Core Customer Data Validation)',
            }),

            'project_description': forms.Textarea(attrs={
                'class': 'form-textarea',
                'placeholder': 'Detail the purpose, scope, and key data domains involved.',
                'rows': '4'  # Sets the height of the textarea
            }),


        }
