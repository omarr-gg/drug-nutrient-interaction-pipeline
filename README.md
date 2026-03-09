# 💊 Drug-Nutrient Interaction ETL Pipeline
> **Pharmacovigilance & Nutritional Interaction Pipeline for Personalized Medicine.**

This project implements a production-grade **ETL (Extract, Transform, Load)** pipeline designed to identify critical drug-nutrient interactions. It leverages **Pandas with the Pyarrow backend** to ensure scalable and memory-efficient processing of clinical datasets.

## 🩺 Clinical Context
Drug-nutrient interactions (DNIs) can significantly alter therapeutic efficacy or cause severe toxicity. This pipeline automates risk detection for critical cases, such as:
* **Warfarin + Vitamin K:** Monitoring alterations in Prothrombin Time (INR).
* **Clinical Safety:** Validating patient medication profiles against evidence-based interaction databases.

## 🚀 Technical Features
* **High-Performance Processing:** Optimized with `pyarrow` for efficient memory management and faster string operations.
* **Data Integrity:** Robust test suite using `pytest` to ensure clinical rule accuracy.
* **Professional Architecture:** Clean separation of concerns between Extraction, Transformation, and Loading logic.

## 🛠️ Installation & Usage

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/omarr-gg/drug-nutrient-interaction-pipeline.git](https://github.com/omarr-gg/drug-nutrient-interaction-pipeline.git)
   cd drug-nutrient-interaction-pipeline