# Enterprise FMCG Data Lakehouse (Medallion Architecture)

An end-to-end, production-ready Data Lakehouse built on Databricks, Delta Lake, and PySpark. It ingests raw FMCG orders data and processes it through a Medallion Architecture (Bronze -> Silver -> Gold) with schema evolution, automated data quality quarantining, and star-schema dimensional modeling.

## 🏗️ Architecture Overview

```text
[ Raw Data Sources ] 
        │
        ▼ (Auto Loader)
[ Bronze Layer ] ──> Raw JSON/CSV Landing
        │
        ▼ (Validation Rules & Window Deduplication)
┌───────────────────────┴───────────────────────┐
│                                               │
▼                                               ▼
[ Silver Orders Table ]               [ Silver Quarantine Table ]
(Valid Customers & Clean Data)       (Failed Records for Audit)
        │
        ▼ (Resilient Left Joins & Coalesced Keys)
[ Gold Layer ] ──> Star Schema Fact & Dimension Tables
        │
        ▼ (Optimization)
[ OPTIMIZE / Z-ORDER ] ──> BI Analytics & Reporting Ready