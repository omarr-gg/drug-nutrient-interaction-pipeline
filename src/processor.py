import pandas as pd
import numpy as np
import logging
import os

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - [%(module)s] - %(message)s'
)

class ClinicalDataProcessor:
    """
    Enterprise-grade data transformation engine.
    Implements strict data validation, clinical business rules, 
    and data quarantine for non-compliant records (Pharma standards).
    """
    def __init__(self, input_path: str, valid_output_path: str, quarantine_output_path: str):
        self.input_path = input_path
        self.valid_output_path = valid_output_path
        self.quarantine_output_path = quarantine_output_path
        self.df = None
        self.quarantine_df = pd.DataFrame() # Holds rejected records

    def load_data(self):
        logging.info("Loading raw data from %s...", self.input_path)
        self.df = pd.read_csv(self.input_path)
        logging.info("Loaded %d raw rows.", len(self.df))

    def validate_and_quarantine(self):
        """
        Strict Data Validation Contract.
        Isolates invalid data instead of silently dropping it.
        """
        logging.info("Executing strict data validation...")
        
        initial_count = len(self.df)
        
        # 1. Deduplication (Keep first, send duplicates to quarantine is complex, we just drop for simplicity here)
        self.df = self.df.drop_duplicates(subset=['patient_id'])
        
        # 2. Identify records with critical missing or invalid data
        # Example: Negative dosages, extreme ages, or unparseable blood pressure
        invalid_mask = (
            (self.df['daily_dosage_mg'] <= 0) | 
            (self.df['age'] < 0) | 
            (self.df['age'] > 120) |
            (self.df['blood_pressure'] == 'ERROR')
        )
        
        # Move invalid rows to quarantine
        self.quarantine_df = self.df[invalid_mask].copy()
        self.quarantine_df['rejection_reason'] = "Failed Data Contract Validation"
        
        # Keep only valid rows in the main pipeline
        self.df = self.df[~invalid_mask].copy()
        
        logging.info("Validation complete. Valid rows: %d. Quarantined rows: %d.", 
                     len(self.df), len(self.quarantine_df))

    def normalize_and_impute(self):
        """Standardizes formats and performs safe imputations."""
        logging.info("Normalizing data formats...")
        
        # Standardize dates safely
        self.df['lab_date'] = pd.to_datetime(self.df['lab_date'], errors='coerce', format='mixed').dt.strftime('%Y-%m-%d')
        
        # Safe imputation (only if medically acceptable, otherwise it should go to quarantine)
        # For PoC, we impute missing weights with the median
        self.df['weight_kg'] = self.df['weight_kg'].fillna(self.df['weight_kg'].median())

    def apply_clinical_rules(self):
        """Applies Pharmacological & Dietary interaction logic."""
        logging.info("Applying clinical business rules...")
        
        def check_interaction(row):
            drug = row['prescribed_drug']
            diet = row['dietary_habit']
            phenotype = row['cyp2c9_phenotype']
            
            if drug == 'Warfarin' and 'Vitamin K' in diet:
                return 'CRITICAL_ALERT: Vit K reduces Warfarin efficacy'
            elif drug == 'Simvastatin' and 'Grapefruit' in diet:
                return 'CRITICAL_ALERT: Grapefruit inhibits CYP3A4'
            elif drug == 'Warfarin' and phenotype == 'Poor Metabolizer':
                return 'WARNING: Requires lower dosage adjustment'
            return 'SAFE'

        self.df['interaction_alert'] = self.df.apply(check_interaction, axis=1)

    def save_outputs(self):
        """Saves both valid data for downstream analysis and quarantined data for auditing."""
        os.makedirs(os.path.dirname(self.valid_output_path), exist_ok=True)
        
        self.df.to_csv(self.valid_output_path, index=False)
        logging.info("Saved %d valid records to %s", len(self.df), self.valid_output_path)
        
        if not self.quarantine_df.empty:
            self.quarantine_df.to_csv(self.quarantine_output_path, index=False)
            logging.warning("Saved %d QUARANTINED records to %s for human review.", 
                            len(self.quarantine_df), self.quarantine_output_path)

    def run_pipeline(self):
        self.load_data()
        self.validate_and_quarantine()
        self.normalize_and_impute()
        self.apply_clinical_rules()
        self.save_outputs()

if __name__ == "__main__":
    processor = ClinicalDataProcessor(
        input_path="data/raw_patients_data.csv",
        valid_output_path="data/processed_valid_patients.csv",
        quarantine_output_path="data/quarantine_patients.csv" # Auditing file
    )
    processor.run_pipeline()