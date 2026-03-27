# Corporate Banking DWH

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-green)

An end-to-end **Corporate Banking Data Warehouse** with synthetic data generation, a multi-layer SQL architecture (OLTP → DWH → Data Mart), an ETL pipeline, and an AI-powered AML risk agent using the DeepSeek LLM.

---

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────────┐
│  data_generator │────▶│  OLTP Layer  │────▶│     ETL Pipeline    │
│   (Faker, 200k  │     │  oltp schema │     │   etl_pipeline.py   │
│   transactions) │     │  clients     │     │   key mapping +     │
└─────────────────┘     │  accounts    │     │   upsert logic      │
                        │  transactions│     └────────┬────────────┘
                        └──────────────┘              │
                                                       ▼
                        ┌──────────────────────────────────────────┐
                        │              DWH Layer (Star Schema)     │
                        │   dim_client  dim_date  dim_tx_type      │
                        │              fact_transactions           │
                        └──────────────────┬───────────────────────┘
                                           │
                              ┌────────────▼────────────┐
                              │      Data Mart           │
                              │  dm_risk schema          │
                              │  client_financial_profile│
                              │  (CTEs, LAG, moving avg) │
                              └────────────┬─────────────┘
                                           │
                              ┌────────────▼─────────────┐
                              │      AI Risk Agent        │
                              │  ai_risk_agent.py         │
                              │  DeepSeek LLM → AML score │
                              └───────────────────────────┘
```

---

## Tech Stack

| Layer        | Technology                          |
|--------------|-------------------------------------|
| Database     | PostgreSQL 16 (Docker)              |
| ORM / DB     | SQLAlchemy 2.0, psycopg2            |
| Data         | pandas 3.0, Faker                   |
| AI / LLM     | DeepSeek API (OpenAI-compatible SDK)|
| Config       | python-dotenv                       |
| Infra        | Docker Compose                      |

---

## Project Structure

```
├── src/
│   ├── sql/
│   │   ├── 01_ddl_schema.sql       # OLTP tables (clients, accounts, transactions)
│   │   ├── 02_dwh_schema.sql       # Star schema (dims + fact_transactions)
│   │   ├── 03_datamarts.sql        # dm_risk.client_financial_profile (window functions)
│   │   └── 04_analytics_views.sql  # Reporting views (monthly, industry, anomaly)
│   ├── python/
│   │   ├── data_generator.py       # Synthetic data generation (Faker, 200k transactions)
│   │   ├── etl_pipeline.py         # OLTP → DWH ETL with surrogate key mapping
│   │   └── ai_risk_agent.py        # AML risk analysis via DeepSeek LLM
│   └── utils/
│       └── get_db_connection.py    # SQLAlchemy engine factory (reads DATABASE_URL)
├── docker-compose.yml
├── requirements.txt
└── .env                            # Not committed — see Environment Variables
```

---

## Quick Start

### 1. Configure environment

Copy and fill in `.env`:

```env
POSTGRES_DB=banking_dwh
POSTGRES_USER=dwh_user
POSTGRES_PASSWORD=dwh_secret
POSTGRES_PORT=5432
DATABASE_URL=postgresql://dwh_user:dwh_secret@localhost:5432/banking_dwh
DEEPSEEK_API_KEY=your_key_here
```

### 2. Start the database

```bash
docker-compose up -d
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Initialize the database schema

Run SQL scripts in order:

```bash
docker exec -i banking_dwh_db psql -U dwh_user -d banking_dwh < src/sql/01_ddl_schema.sql
docker exec -i banking_dwh_db psql -U dwh_user -d banking_dwh < src/sql/02_dwh_schema.sql
```

### 5. Generate synthetic data

```bash
python -m src.python.data_generator
```

Generates: 100 clients, ~200 accounts, 200,000 transactions (with 1% anomaly rate).

### 6. Run the ETL pipeline

```bash
python -m src.python.etl_pipeline
```

Loads `dim_client` and `fact_transactions` into the DWH with surrogate key mapping.

### 7. Build the data mart

```bash
docker exec -i banking_dwh_db psql -U dwh_user -d banking_dwh < src/sql/03_datamarts.sql
docker exec -i banking_dwh_db psql -U dwh_user -d banking_dwh < src/sql/04_analytics_views.sql
```

### 8. Run the AI risk agent

```bash
python -m src.python.ai_risk_agent
```

---

## Data Pipeline

### Stage 1 — OLTP Layer
Raw operational tables mimicking a real banking system:
- `oltp.clients` — corporate clients with INN, industry, region
- `oltp.accounts` — bank accounts linked to clients
- `oltp.transactions` — 200k payment records (INCOME/OUTCOME) with free-text descriptions

### Stage 2 — DWH Layer (Star Schema)
Dimensional model optimized for analytics:
- `dwh.dim_client` — client dimension with surrogate keys (SCD Type 1)
- `dwh.dim_date` — date dimension covering 2020–2030
- `dwh.dim_transaction_type` — transaction type lookup (INCOME / OUTCOME)
- `dwh.fact_transactions` — central fact table with amount in RUB and ETL timestamp

### Stage 3 — Data Mart
`dm_risk.client_financial_profile` is built with advanced SQL:
- Monthly income/outcome aggregation per client
- 3-month moving average income (smoothing)
- LAG window function to detect income trend (GROWTH / STABLE / DECLINE / NEW)
- Anomaly flag: max transaction > 10× average transaction for that client
- Aggregated transaction keyword sample for NLP/LLM prompts

### Stage 4 — Analytics Views
Three reporting views in `dwh_views` schema:
- `v_monthly_client_summary` — cashflow trends per client per month
- `v_industry_risk_summary` — anomaly rate and trend distribution by industry
- `v_top_anomaly_clients` — clients with suspicious transactions ranked by anomaly ratio

---

## AI Risk Agent

`ai_risk_agent.py` connects the data mart to a real LLM for AML analysis:

1. Loads client profiles from `dm_risk.client_financial_profile`
2. Preprocesses data: fills nulls, maps trend labels to numeric scores, flags risky clients (`has_anomaly_tx = 1` AND `trend_score < 0`)
3. Runs portfolio analytics by industry
4. Sends the top 3 risky clients to **DeepSeek** (`deepseek-chat`) with a structured AML prompt
5. The LLM answers: should additional documents be requested? (Yes/No + reason)

The prompt includes: company name, industry, income trend, anomaly flag, and sampled transaction descriptions.

---

## Environment Variables

| Variable           | Description                                      |
|--------------------|--------------------------------------------------|
| `POSTGRES_DB`      | Database name                                    |
| `POSTGRES_USER`    | PostgreSQL user                                  |
| `POSTGRES_PASSWORD`| PostgreSQL password                              |
| `POSTGRES_PORT`    | Host port mapped to container's 5432             |
| `DATABASE_URL`     | Full SQLAlchemy connection string                |
| `DEEPSEEK_API_KEY` | API key for DeepSeek (openai-compatible endpoint)|
