import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
import logging
import os

# Professional logging configuration
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - [%(module)s] - %(message)s'
)

class AdvancedClinicalDataGenerator:
    """
    Advanced synthetic clinical data generator.
    Simulates complex Electronic Health Records (EHR) including biomarkers, 
    unstructured clinical notes, and intentional statistical noise (dirty data) 
    to mimic a real-world Data Lake environment in the pharmaceutical industry.
    """
    def __init__(self, num_records: int):
        self.num_records = num_records
        
        # Target drugs for pharmacological interactions
        self.drugs = ["Warfarin", "Simvastatin", "Levothyroxine", "Ibuprofen"]
        
        # Dietary habits with known interaction profiles
        self.diets = [
            "High Vitamin K", "Grapefruit Juice Daily", 
            "Calcium Supplements", "Standard Mediterranean", "Vegan"
        ]
        
        # Genetic metabolization profiles (Pharmacogenomics)
        self.cyp450_profiles = ["Extensive Metabolizer", "Poor Metabolizer", "Ultrarapid Metabolizer"]
        
        # Patient comorbidities
        self.conditions = ["Hypertension", "Type 2 Diabetes", "Osteoarthritis", "None", "Atrial Fibrillation"]

    def _generate_doctor_note(self, drug: str, diet: str) -> str:
        """Simulates unstructured, potentially messy clinical notes."""
        notes = [
            f"Pt prescribed {drug}. Advised to avoid {diet}.",
            f"Patient reports taking {drug} regularly. Diet: {diet}.",
            f"Review in 3 months. Meds: {drug} - pt eating {diet} freq.",
            # Messy/incomplete notes to challenge the downstream ETL process
            f"{drug} 1x/day. notes: {diet} intake high",
            "No changes to medication." if random.random() > 0.8 else f"Rx: {drug}."
        ]
        return random.choice(notes)

    def _generate_patient(self) -> dict:
        """Generates a single patient record with integrated clinical logic and noise."""
        drug = random.choice(self.drugs)
        diet = random.choice(self.diets)
        
        # Simulate inconsistent date formats (Classic Dirty Data)
        date_format = random.choice(["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"])
        lab_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime(date_format)

        # Biomarkers with Gaussian distribution
        # Normal AST/ALT: 10-40 U/L. Introducing outliers for hepatotoxicity.
        ast_level = round(random.gauss(25, 10), 1)
        if random.random() > 0.95: 
            ast_level *= random.uniform(3.0, 10.0) # Severe outlier
        
        # Normal INR: 1.0. Target for Warfarin patients: 2.0 - 3.0
        inr_level = round(random.gauss(1.1, 0.2), 2)
        if drug == "Warfarin":
            inr_level = round(random.gauss(2.5, 0.8), 2)

        record = {
            "patient_id": str(uuid.uuid4()),
            "age": random.randint(18, 95),
            "gender": random.choice(["M", "F", "Other", "Unknown"]),
            "ethnicity": random.choice(["Caucasian", "Hispanic", "African", "Asian", "Unknown"]),
            "weight_kg": round(random.gauss(75.0, 15.0), 1) if random.random() > 0.05 else None, # 5% Nulls
            "height_cm": round(random.gauss(170.0, 10.0), 1),
            "primary_condition": random.choice(self.conditions),
            "cyp2c9_phenotype": random.choice(self.cyp450_profiles),
            "prescribed_drug": drug,
            "daily_dosage_mg": round(random.uniform(5.0, 150.0), 2),
            "dietary_habit": diet,
            "lab_date": lab_date,
            "ast_enzyme_ul": max(1.0, ast_level), # Prevent negative values
            "alt_enzyme_ul": max(1.0, round(random.gauss(25, 12), 1)),
            "inr_level": max(0.5, inr_level),
            # Intentional anomalies (e.g., negative blood pressure or string errors)
            "blood_pressure": f"{random.randint(90, 180)}/{random.randint(60, 120)}" if random.random() > 0.02 else "ERROR",
            "doctor_notes": self._generate_doctor_note(drug, diet)
        }
        return record

    def generate_and_save(self, output_path: str):
        """Generates the dataset, injects duplicates, and saves to CSV."""
        logging.info("Starting generation of %d complex clinical profiles...", self.num_records)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        data = [self._generate_patient() for _ in range(self.num_records)]
        df = pd.DataFrame(data)
        
        # Inject duplicate rows to test deduplication logic in ETL
        duplicates = df.sample(frac=0.02)
        df = pd.concat([df, duplicates], ignore_index=True)
        
        df.to_csv(output_path, index=False)
        logging.info("Advanced dataset successfully saved. Total rows (including duplicates): %d", len(df))

if __name__ == "__main__":
    # Generate 10k records for the Proof of Concept (PoC)
    generator = AdvancedClinicalDataGenerator(num_records=10000)
    generator.generate_and_save("data/raw_patients_data.csv")