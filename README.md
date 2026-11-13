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

## Layout

| Path | Contents |
|---|---|
| `dags/` | `flight_price_pipeline` DAG |
| `src/ingestion` | CSV to MySQL loader (with rejected-row handling) |
| `src/validation` | Data-quality checks |
| `src/transformation` | Feature engineering, fare checks, record hash |
| `src/loading` | Bulk load into PostgreSQL |
| `src/analytics` | KPI queries and KPI storage |
| `sql/` | MySQL, PostgreSQL and KPI schemas |
| `tests/` | `python -m pytest` |

Each stage can also be run on its own from the project root, for example
`python -m src.ingestion.run_mysql_ingestion`.
