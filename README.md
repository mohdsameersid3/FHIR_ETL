# FHIR ETL Pipeline using Databricks (Medallion Architecture)

## Project Overview

This project implements an end-to-end ETL pipeline for ingesting healthcare data from the public **FHIR (Fast Healthcare Interoperability Resources)** API into a Medallion Lakehouse Architecture using **Databricks Community Edition**, **PySpark**, and **Delta Lake**.

The solution was developed as part of the **FHIR API Data Ingestion & Analytics Assignment** and demonstrates modern data engineering practices including:

* Incremental API ingestion
* Pagination support
* Metadata tracking
* Medallion Architecture (Raw → Bronze → Silver → Gold)
* Slowly Changing Dimension (SCD Type 2)
* Configuration-driven processing
* Modular and reusable pipeline design
* Power BI reporting using Databricks SQL Warehouse

---

# Business Requirements

The project satisfies the following business requirements:

* Incremental ingestion from the public FHIR API
* Support for pagination while extracting data
* Raw API response archival (Unitiy Catalog Volume storages)
* Bronze, Silver and Gold layer implementation
* Metadata enrichment for auditability
* Historical versioning using SCD Type 2
* Modular reusable code with configuration-driven mappings
* End-to-end orchestration
* Analytics-ready Gold layer for reporting

---

# Technology Stack

| Technology                   | Purpose                       |
| ---------------------------- | ----------------------------- |
| Python                       | Core Programming              |
| PySpark                      | Distributed Data Processing   |
| Databricks Community Edition | Lakehouse Platform            |
| Delta Lake                   | Bronze, Silver & Gold Storage |
| YAML                         | Configuration Management      |
| Git & GitHub                 | Version Control               |
| Power BI                     | Reporting & Dashboarding      |

---

# Source System

FHIR Public API

https://hapi.fhir.org/baseR4/swagger-ui/

Resources Ingested

* Patient
* Encounter
* Observation
* Condition

---

# Project Structure

```text
FHIR_ETL
│
├── config/
│   ├── config.yaml
│   └── settings.py
│
├── data/
│   ├── raw/
│   ├── bronze/
│   └── checkpoint/
│
├── logs/
│
├── src/
│   ├── extraction/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── pipeline/
│   └── common/
│
├── tests/
│
├── requirements.txt
└── README.md
```

---

# Solution Architecture

```text
                 FHIR REST API
                       │
                       ▼
          Incremental API Extraction
          (Pagination + Checkpoint)
                       │
                       ▼
               Raw Layer (JSON)
          (Unity Catalog Volumes for storage)
                       │
                       ▼
          Bronze Layer (Parquet/Delta)
      Metadata + Audit Information Added
                       │
                       ▼
          Silver Layer (Cleaned Data)
      Deduplication + SCD Type 2 Logic
                       │
                       ▼
         Gold Layer (Analytics Tables)
                       │
                       ▼
      Databricks SQL Warehouse
                       │
                       ▼
                 Power BI Dashboard
```

---

# Medallion Architecture

## Raw Layer

The Raw Layer stores the API responses exactly as received from the FHIR API.

Characteristics

* JSON responses stored without modification
* Folder structure partitioned by Resource and Extraction Date
* Supports data replay and auditing

Example

```
data/raw/Patient/2026-07-18/
```

---

## Bronze Layer

The Bronze Layer converts raw JSON into structured Parquet/Delta format while preserving all source information.

Additional metadata captured

* extraction_timestamp
* api_url_or_params

Bronze acts as the persistent landing layer for downstream transformations.

---

## Silver Layer

The Silver Layer performs business transformations.

Implemented transformations include

* Column mapping
* Data cleansing
* Deduplication
* Standardization
* Slowly Changing Dimension (SCD Type 2)

Historical versions are maintained whenever changes are detected in business attributes.

---

## Gold Layer

The Gold Layer contains analytics-ready tables optimized for reporting.

The Gold tables are consumed directly by Power BI through Databricks SQL Warehouse.

---

# Incremental Data Ingestion

The extraction framework supports incremental loading by maintaining checkpoints.

Features

* Pagination support
* Incremental API calls
* Checkpoint management
* Batch-wise processing
* Metadata generation
* Raw response archival

---

# Metadata Management

Every batch contains metadata for auditing.

Captured metadata includes

* Extraction Timestamp
* API URL
* Batch Information
* Processing Date

This enables complete data lineage and traceability.

---

# Slowly Changing Dimension (SCD Type 2)

Historical versions are preserved in the Silver Layer.

Each record maintains

* Current Version Flag
* Effective Start Date
* Effective End Date

Whenever a business attribute changes

* Previous record is expired
* New version is inserted

This preserves the complete history of every business entity.

---

# Data Model

The project models four core FHIR entities.

```
Patient
   │
   ├───────────────┐
   │               │
   ▼               ▼
Encounter      Condition
   │               ▲
   │               │
   ▼               │
Observation ───────┘
```

## Relationships

### Patient

Primary entity representing an individual.

Primary Key

```
patient_id
```

---

### Encounter

Represents a patient visit.

Relationship

```
Patient (1)
      │
      │
      ▼
Encounter (Many)
```

Foreign Key

```
patient_reference
```

---

### Observation

Clinical observations recorded during an encounter.

Relationships

```
Patient (1)
      │
      ▼
Observation (Many)

Encounter (1)
      │
      ▼
Observation (Many)
```

Foreign Keys

* patient_reference
* encounter_reference

---

### Condition

Medical conditions diagnosed during patient care.

Relationships

```
Patient (1)
      │
      ▼
Condition (Many)

Encounter (1)
      │
      ▼
Condition (Many)
```

Foreign Keys

* patient_reference
* encounter_reference

---

# Pipeline Execution Flow

The orchestration executes resources in the following sequence

```
extractions (extraction_pipeline.py) -> (raw data in dated folder structure
     │                                   used unity catlog volumes to store)
     ▼
Bronze Load (bronze_pipeline.py) -> (exploding and loading as delta tables)
     │
     ▼
Silver load (silver_pipeline.py) -> (transaformation and SDC type 2 handling)
     │
     ▼
Gold Load (gold_loader.py) -> (curated adn aggredated overwritten tables)
     │ 
     ▼ 
Analytics (SQL Datawarehouse) -> PBI model
```

Each stage completes before the next begins to maintain referential integrity.

---

# Configuration Driven Design

The pipeline is fully configuration driven.

Resource mappings, business keys and target tables are maintained in

```
config/config.yaml
```

This eliminates hardcoded transformations and simplifies onboarding of new FHIR resources.

---

# Logging

Centralized logging is implemented throughout the application.

Separate log files are generated for

* Extraction
* Bronze Loading
* Silver Processing
* Gold Loading
* Checkpoint Processing
* Pagination

---

# Testing

Unit tests are included for major components including

* Configuration
* API Client
* Pagination
* Raw Writer
* Bronze Loader
* Metadata Enrichment
* Hash Calculation
* Silver Loader
* Master Pipeline

---

# Power BI Integration

The Gold Layer is exposed through a Databricks SQL Warehouse.

Power BI connects using **DirectQuery**, enabling interactive dashboards on top of the Gold Layer without importing data.

---

# How to Run

## Install dependencies

```bash
pip install -r requirements.txt
```

## Execute the pipeline

```bash
python src/pipeline/master_pipeline.py
```

---

# Future Enhancements

* Automated scheduling using Databricks Workflows
* CI/CD using GitHub Actions or Azure DevOps
* Data Quality validation framework

---

# Author

**Mohammad Sameer Uddin**

Developed as part of the **FHIR API Data Ingestion & Analytics Assignment** demonstrating an end-to-end Lakehouse ETL pipeline using Databricks, PySpark, Delta Lake and Power BI.
