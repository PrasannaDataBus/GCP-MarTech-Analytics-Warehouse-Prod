# 🧠 GCP MarTech Analytics Warehouse

**Data Engineering & Marketing Analytics Automation Pipeline**

This repository contains the codebase for the **Google Cloud Platform (GCP) Marketing Technology Analytics Warehouse**. It is designed to automate the extraction, transformation, and loading (ETL) of multi-channel marketing data into **BigQuery** for advanced analytics, reporting, and budget optimization.

---

## 📦 Overview

The GCP MarTech Analytics Warehouse integrates diverse marketing data sources (including **Google Ads**, **Google Analytics 4**, **Meta Ads**, **LinkedIn Ads**) into a **centralized BigQuery warehouse**.

### Core Pipeline Mechanics

**1. Extraction:**
Raw data is extracted using custom Python scripts via official **APIs** for each platform. The data is ingested directly into a raw dataset in BigQuery.

**2. Loading Strategy (Google Ads Example):**
The Google Ads pipeline comprises **17 distinct extraction modules** producing 17 raw tables. The load process implements advanced data engineering logic:

-   **Historical Backfill:** Captures the past 3 years of data to enable Year-over-Year (YoY) and Year-to-Date (YTD) performance comparison.
-   **Incremental Loading:** Efficiently appends new daily records to the historical dataset.
-   **Rolling Lookback Window (CDC):** Implements a 14-day lookback window to capture **Change Data (CDC)** and **Attribution Lag** (e.g., conversions attributed days after the ad click).
-   **Partitioning:** Handled within the Python load step; tables are partitioned by `DATE` for query optimization.
-   **Clustering:** Tables utilize clustered columns (e.g., `Campaign ID`, `Ad Group ID`) to reduce query costs and latency.

### Orchestration Architecture

The pipeline is orchestrated using **Apache Airflow** deployed via two distinct **Docker containers**:

-   **Airflow-Dev:** Contains 17 DAGs using **SQLite**. No scheduling enabled. Used strictly for development testing and DAG validation.
-   **Airflow-Prod:** Contains 17 DAGs using **PostgreSQL**. Daily scheduling is active. Used for deploying and orchestrating core ETL processes.

---

## 🏗️ Architecture Summary

| Layer | Description |
|--------|--------------|
| **Extract** | Pulls data from APIs (Google Ads, GA4, Meta Ads, LinkedIn Ads, TikTok Ads etc.) using official SDKs and Python scripts. |
| **Transform** | Performs cleaning, aggregation, and enrichment of raw datasets. |
| **Load** | Writes structured data into BigQuery datasets following best schema practices. |
| **Orchestration** | Apache Airflow manages DAGs, task dependencies, retries, and SLAs. |
| **Secrets Management** | Environment variables securely store credentials. |

## 📅 Project Roadmap & Scope

This is an active, ongoing engineering initiative.

- **✅ Phase 1 (Complete):** Google Ads Extraction & Loading (17 Tables).
- **🔄 Phase 2 (In Progress):** Meta Ads (Facebook/Instagram) API Integration.
- **🔜 Phase 3 (Planned):** Transformation Layer using **dbt** (Data Build Tool) to create Gold/Mart datasets.
- **📊 Phase 4 (Analytics):** BI connection via **Looker** to visualize ROAS, ROI, and facilitate budget pacing/optimization.

---

## 📁 Folder Structure

```text
├── dags/ # Airflow DAG definitions (Dev/Prod)
├── extract/ # Source data extraction scripts (Python)
├── load/ # BigQuery load operations & schema definitions
├── transform/ # Transformation and cleaning logic
├── utils/ # Helper modules (logging, config, etc.)
└── README.md # Project documentation
├── best_practices.txt / # GCP & Engineering Guidelines
├── git_devops # CI/CD and version control references
├── requirements.txt # Python dependencies
├── test.py # perform various tests
```
---

## ⚙️ Airflow & GCP Integration

- **Containerization:** Airflow runs on Docker (`Airflow-Docker-Dev` / `Airflow-Docker-Prod`)  
- **Data Warehousing:** BigQuery serves as the central repository.
- **SDK Integration:** Utilizes the official `google-ads` Python library.  
- **Idempotency:** Incremental logic ensures data consistency without duplication.  
- **Environment Variables:** Separate .env configurations for Dev and Prod credentials.

---

## 🚀 Development & Deployment

The setup follows a strict two-environment architecture:

### 🧪 Dev Environment (`GCP-MarTech-Analytics-Warehouse-Dev`)

Used for DAG authoring, unit testing, and incremental load validation.

Run using:
```bash
docker compose run --rm airflow-init
docker compose up -d
```

Admin Access: http://localhost:8081

### 🏭 Production Environment (GCP-MarTech-Analytics-Warehouse-Prod)

Used for stable, scheduled data pipelines and automated reporting feeds.

Run using:
```bash
docker compose up -d
```

Admin Access: http://localhost:8080

🔒 Security Policy

- **Secrets Management:** `.env`, `.yaml`, and `.json` files are strictly excluded via .gitignore.
- **IAM**: GCP Service Accounts utilize Least Privilege access principles.
- **Authentication:** All API connections are authenticated via secure OAuth2 flows or encrypted JSON key files
- **Audit:** Access logs are reviewed and API keys are rotated quarterly.

🤝 Code of Conduct

We strive to maintain high engineering standards:

- Clean, modular, and version-controlled code.
- Clear commit messages with semantic versioning.
- All DAGs must pass Dev validation before merging to Prod.
- Documentation must be updated with every major feature release.

🧱 Versioning

- Dev	master	v1.x.x	Active development and testing
- Prod master	v1.x.x	Stable and verified releases

🧠 Author & Maintainer

Prasanna - *Data Engineer & Analytics Specialist*

📧 prasanna.uthamaraj@informa.com

🌐 GitHub: PrasannaDataBus

🏁 License

- This repository and its contents are proprietary to Informa PLC / PrasannaDataBus.
- Unauthorized redistribution, public sharing of API credentials, business logic, or proprietary connectors is strictly prohibited.
