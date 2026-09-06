# Enterprise FMCG Data Lakehouse Platform

[![CI/CD Pipeline](https://github.com/LunarByteFlow/complete_fmcg_pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/LunarByteFlow/complete_fmcg_pipeline/actions)
![Databricks](https://img.shields.io/badge/Databricks-Asset_Bundles-red?logo=databricks)
![PySpark](https://img.shields.io/badge/PySpark-3.5+-orange?logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-blue)
![Python](https://img.shields.io/badge/Python-3.10+-yellow?logo=python)

An end-to-end, modular PySpark Data Lakehouse pipeline designed for Fast-Moving Consumer Goods (FMCG) analytics. Built on Databricks using a multi-hop Medallion Architecture (Bronze → Silver → Gold), featuring stateful incremental ingestion via Databricks Auto Loader, dynamic dynamic schema evolution, Dead-Letter Queue (DLQ) quarantine routing, Kimball Star Schema modeling, and automated CI/CD unit testing via GitHub Actions and PyTest.

---

## 🏗️ Architecture Overview

```mermaid
flowchart LR
    subgraph Source ["Raw Data Storage"]
        S3["AWS S3 / Landing Bucket\n(CSV / JSON Drop)"]
    end

    subgraph Databricks ["Databricks Lakehouse Engine (PySpark)"]
        AL["Auto Loader\n(cloudFiles)"]
        
        subgraph Medallion ["Medallion Architecture"]
            Bronze[("Bronze Delta Table\n(Raw + Ingestion Metadata)")]
            
            subgraph QualityFilter ["Quality & Validation Engine"]
                Val{"Schema & FK\nValidation"}
                DLQ[("Silver DLQ Quarantine\n(Malformed / Invalid Keys)")]
            end
            
            Silver[("Silver Delta Table\n(Cleaned, Deduplicated, Typed)")]
            Gold[("Gold Delta Layer\n(Kimball Star Schema Fact & Dims)")]
        end
      
        S3 --> AL --> Bronze
        Bronze --> Val
        Val -- "Failed Records" --> DLQ
        Val -- "Validated Records" --> Silver
        Silver --> Gold
    end

    subgraph Analytics ["Downstream BI & Analytics"]
        PBI["Power BI / Databricks SQL Engine"]
    end

    Gold --> PBI
