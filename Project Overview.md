# 🧠 GCP MarTech Analytics Warehouse

**Data Engineering & Marketing Analytics Automation Pipeline**

This repository contains the codebase for the **Google Cloud Platform (GCP) Marketing Technology Analytics Warehouse**, designed to automate the extraction, transformation, and loading (ETL) of multi-channel marketing data into **BigQuery** for analytics, reporting, and performance optimization.

---

## 📦 Overview

The GCP MarTech Analytics Warehouse integrates multiple marketing data sources (such as **Google Ads**, **Google Analytics**, and other APIs) into a **centralized BigQuery warehouse** using **Apache Airflow** for orchestration.

The setup follows a **two-environment architecture**:
- 🧪 **Dev Environment:** Used for DAG testing, incremental load validation, and pipeline debugging.
- 🏭 **Prod Environment:** Stable deployment of tested DAGs for daily scheduled runs and reporting feeds.

---

## 🏗️ Architecture Summary

| Layer | Description |
|--------|--------------|
| **Extract** | Pulls data from APIs (Google Ads, GA4, Meta Ads, LinkedIn Ads, TikTok Ads etc.) using official SDKs and Python scripts. |
| **Transform** | Performs cleaning, aggregation, and enrichment of raw datasets. |
| **Load** | Writes structured data into BigQuery datasets following best schema practices. |
| **Orchestration** | Apache Airflow manages DAGs, dependencies, retries, and scheduling. |
| **Secrets Management** | Environment variables securely store credentials. |

---

## 📁 Folder Structure

├── dags/ # Airflow DAG definitions (Dev/Prod)
├── extract/ # Source data extraction scripts
├── load/ # BigQuery load operations
├── transform/ # Transformation and cleaning logic
├── utils/ # Helper modules (logging, config, etc.)
└── README.md # Project documentation
├── best_practices.txt / # GCP Best Practices Documentation
├── git_devops # git code references
├── requirements.txt # Python dependencies
├── test.py # perform various tests

---

## ⚙️ Airflow & GCP Integration

- **Airflow** is containerized via Docker (`Airflow-Docker-Dev` / `Airflow-Docker-Prod`)  
- **BigQuery** serves as the data warehouse for all marketing datasets  
- **Google Ads API** is integrated using the official `google-ads` SDK  
- **Incremental Loading** ensures only new and updated data are inserted each run  
- **Environment Variables** manage separate credentials for Dev/Prod

---

## 🚀 Development & Deployment

### 🧪 Dev Environment (`GCP-MarTech-Analytics-Warehouse-Dev`)
Used for:
- DAG authoring and testing
- Incremental load validation
- Local BigQuery integration testing

Run using:
```bash
docker compose run --rm airflow-init
docker compose up -d
```

Admin Access: http://localhost:8081

### 🏭 Production Environment (GCP-MarTech-Analytics-Warehouse-Prod)
Used for:
- Stable, scheduled data pipelines
- Automatic daily extractions
- Dashboard data refreshes

Run using:
```bash
docker compose up -d
```

Admin Access: http://localhost:8080

🔒 Security Policy

- Secrets (.env, .yaml, .json) are excluded via .gitignore
- GCP service accounts use least-privilege access
- No raw credentials or tokens are committed to the repository
- All API connections (Google Ads, GA4, BigQuery, Meta Ads, LinkedIn Ads, TikTok Ads) are authenticated via secure OAuth2 or JSON key files
- Review access logs and rotate keys every quarter

🤝 Code of Conduct

We strive to maintain:

- Clean, modular, and version-controlled code
- Clear commit messages with semantic versioning
- Tested DAGs before merging to Prod
- Respectful collaboration and documentation clarity

🧱 Versioning

- Dev	master	v1.x.x	Feature development and testing
- Prod master	v1.x.x	Stable and verified releases

🧠 Author & Maintainer

Prasanna

📧 prasanna.uthamaraj@informa.com

🌐 GitHub: PrasannaDataBus

🏁 License

- This repository is proprietary to Informa PLC / PrasannaDataBus.
- Unauthorized redistribution or public sharing of API credentials, business logic, or proprietary connectors is prohibited.
