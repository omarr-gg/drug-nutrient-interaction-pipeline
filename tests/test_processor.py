import pandas as pd
import pytest
from src.processor import ClinicalDataProcessor

class TestClinicalRules:
    """
    Test suite for clinical and pharmacological business rules.
    Ensures that drug-nutrient interactions trigger the correct alerts.
    """
    
    @pytest.fixture
    def processor_instance(self):
        """Creates a fresh instance of the processor for testing."""
        # We don't need real files for this specific test, just the logic
        return ClinicalDataProcessor("dummy.csv", "dummy_out.csv", "dummy_quarantine.csv")

    def test_warfarin_vitamin_k_interaction(self, processor_instance):
        """
        GIVEN a patient taking Warfarin with a High Vitamin K diet
        WHEN the clinical rules are applied
        THEN a CRITICAL_ALERT must be generated.
        """
        # 1. Setup: Create a mock dataframe with one specific patient
        mock_data = {
            'patient_id': ['123'],
            'prescribed_drug': ['Warfarin'],
            'dietary_habit': ['High Vitamin K'],
            'cyp2c9_phenotype': ['Extensive Metabolizer']
        }
        processor_instance.df = pd.DataFrame(mock_data)
        
        # 2. Action: Run the business logic
        processor_instance.apply_clinical_rules()
        
        # 3. Assert: Verify the outcome is exactly as expected
        result_alert = processor_instance.df.iloc[0]['interaction_alert']
        assert 'CRITICAL_ALERT' in result_alert, f"Expected CRITICAL_ALERT, but got {result_alert}"

    def test_safe_interaction(self, processor_instance):
        """
        GIVEN a patient with no known interactions (Ibuprofen + Standard Diet)
        WHEN the clinical rules are applied
        THEN the alert status must be SAFE.
        """
        mock_data = {
            'patient_id': ['456'],
            'prescribed_drug': ['Ibuprofen'],
            'dietary_habit': ['Standard Mediterranean'],
            'cyp2c9_phenotype': ['Extensive Metabolizer']
        }
        processor_instance.df = pd.DataFrame(mock_data)
        
        processor_instance.apply_clinical_rules()
        
        result_alert = processor_instance.df.iloc[0]['interaction_alert']
        assert result_alert == 'SAFE', f"Expected SAFE, but got {result_alert}"