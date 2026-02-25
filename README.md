# 📊 Value at Risk (VaR) ETL Pipeline – Databricks Asset Bundle

## 🚀 Overview

This project implements a production-style ETL pipeline to compute **Historical Value at Risk (VaR)** for equity instruments using a **Medallion Architecture (Bronze → Silver → Gold)** on Databricks.

The pipeline is fully automated using:

- Databricks Asset Bundles  
- GitHub Actions (CI/CD)  
- Delta Lake  
- PySpark  
- Unit testing with pytest  
- Linting via ruff (uv-based environment)  

This project simulates how a quantitative risk team would structure a scalable, environment-aware, production-ready data platform.

---

## 🏗 Architecture

The system follows a Medallion Architecture:

### 🔹 Bronze Layer
- Ingests raw stock data (Auto Loader / external source)
- Stores raw JSON or structured market data
- Append-only Delta table

### 🔹 Silver Layer
- Cleans and standardizes data
- Computes log returns
- Handles nulls and data validation
- Stores curated return dataset

### 🔹 Gold Layer
- Computes Historical VaR
- Parameterized by:
  - `as_of_date`
  - `look_back`
  - `alpha`
- Supports upsert / merge logic
- Produces production-ready risk output table

---

## 📐 VaR Methodology

This project implements **Historical Simulation VaR**:

VaR(α) = - Quantile(1 - α) of returns

Parameters:

| Parameter     | Description                                   |
|--------------|-----------------------------------------------|
| `alpha`      | Confidence level (e.g., 0.95)                 |
| `look_back`  | Rolling window size (e.g., 252 trading days)  |
| `as_of_date` | Evaluation date                               |

The design supports backfilling and multi-day computation.

---

## ⚙️ Project Structure

```text
var_etl/
│
├── pipeline/
│   ├── bronze_ingestion.py
│   ├── silver_returns.py
│   └── gold_var_by_ticker.py
│
├── tests/
│   ├── test_returns.py
│   └── test_var.py
│
├── databricks.yml
├── pyproject.toml
├── uv.lock
└── .github/workflows/
 ```

---

## 🔄 CI/CD Workflow

### 🧪 On Feature Branch Push
- Install dependencies using uv
- Run linting (ruff)
- Run unit tests (pytest)
- Deploy to **dev workspace**

### 🚀 On Merge to Main
- Validate Databricks bundle
- Deploy to **prod workspace**

This mimics real-world Dev → Prod promotion workflows.

---

## 🧪 Testing Strategy

Unit tests validate:

- Log return calculation
- VaR quantile correctness
- Edge cases (null values, insufficient window)
- Deterministic Spark transformations

All transformations are implemented as pure Python functions, making them testable outside notebook context.

---

## 🔐 Environment Management

Environment separation is handled via:

- Databricks Bundle targets
- GitHub Secrets:
  - `DATABRICKS_TOKEN`
  - `DATABRICKS_HOST`
- Environment-aware deployment (`dev`, `prod`)

No credentials are stored in code.

---

## 🛠 Tech Stack

- Databricks  
- Delta Lake  
- PySpark  
- Python 3.11  
- uv (package manager)  
- pytest  
- ruff  
- GitHub Actions  
- Databricks Asset Bundles  

---

## 📊 Example Output

| symbol | as_of_date | alpha | look_back | var     |
|--------|------------|-------|-----------|---------|
| AAPL   | 2026-02-13 | 0.95  | 252       | -0.0213 |

---

## 🎯 Design Principles

- Clean separation of layers
- Idempotent transformations
- Parameterized execution
- Production-grade CI/CD
- Environment isolation
- Testable Spark transformations
- Extensible architecture

---



