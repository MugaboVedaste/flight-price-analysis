# Flight Price Analysis

Airflow pipeline that loads the *Flight Price Dataset of Bangladesh* into MySQL
(staging), validates and transforms it, loads it into PostgreSQL (analytics) and
stores the KPI tables.

```
CSV -> MySQL staging -> validate -> transform -> PostgreSQL -> KPI tables -> Metabase
```

Full documentation (architecture, tasks, validation rules, KPI definitions,
challenges): **[docs/report.md](docs/report.md)**

## Quick start

```bash
cp .env.example .env              # fill in credentials and FERNET_KEY
docker compose up -d              # Airflow at http://localhost:8080
# optional local databases instead of host databases:
docker compose --profile local-db up -d
```

Then create the Airflow connections `mysql_flight_staging`,
`postgres_flight_analytics` and (optional) `smtp_default`, and trigger the
`flight_price_pipeline` DAG.

## Project structure

```
flight-price-analysis/
├── dags/
│   ├── flight_price_pipeline.py        # Main DAG (5 tasks)
│   └── test_database_connections.py    # Helper DAG: checks MySQL/PostgreSQL connections
├── src/
│   ├── ingestion/
│   │   ├── mysql_loader.py             # CSV -> MySQL staging (rejects malformed rows)
│   │   └── run_mysql_ingestion.py      # Run ingestion locally
│   ├── validation/
│   │   ├── data_quality.py             # Data-quality checks
│   │   └── run_data_quality_checks.py  # Run validation locally
│   ├── transformation/
│   │   ├── transform.py                # Feature engineering, fare checks, record hash
│   │   └── run_transformation.py       # MySQL -> processed CSV
│   ├── loading/
│   │   ├── postgres_loader.py          # Processed CSV -> PostgreSQL (bulk, idempotent)
│   │   ├── run_postgres_loading.py     # Run loading locally
│   │   └── backfill_record_hashes.py   # One-off: fill record_hash on old rows
│   └── analytics/
│       ├── kpi_queries.py              # The KPI SQL queries
│       ├── kpi_storage.py              # Saves KPIs into kpi_* tables
│       └── run_analytics.py            # Compute and store all KPIs
├── sql/
│   ├── mysql_schema.sql                # Staging + rejected-rows tables
│   ├── postgres_schema.sql             # Analytics table and indexes
│   └── kpi_schema.sql                  # KPI result tables
├── config/
│   ├── database.py                     # Local DB connections (reads .env)
│   └── airflow.cfg                     # Airflow configuration
├── data/
│   ├── Flight_Price_Dataset_of_Bangladesh.csv   # Source dataset
│   └── processed/                      # Generated transformed CSV (git-ignored)
├── tests/                              # pytest unit tests
├── docs/
│   └── report.md                       # Full project report
├── docker-compose.yaml                 # Airflow, Metabase, optional local databases
├── requirements.txt                    # Local development dependencies
├── requirements-airflow.txt            # Airflow provider packages
├── .env.example                        # Template for .env (never commit .env)
├── pytest.ini
└── README.md
```

Folders created at runtime and not committed: `logs/`, `plugins/`, `.venv/`.

## Running a single stage

Each stage can also be run on its own from the project root, for example
`python -m src.ingestion.run_mysql_ingestion`.
