# Customer360 Retail Analytics

**Databricks Medallion Pipeline · Azure · PySpark · Delta Lake · Power BI**

End-to-end retail analytics: 2,000 e-commerce orders + Azure SQL customers + OpenWeatherMap weather data, processed Bronze → Silver → Gold and served via Power BI.

---

## Data Sources (3)

| # | Source | Type | Join Key |
|---|--------|------|----------|
| 1 | `Ecommerce_2000.csv` | 2,000 orders, 23 cols | `CustomerID` |
| 2 | `dbo.customers` | Azure SQL (JDBC) | `CustomerID` |
| 3 | OpenWeatherMap API | Weather by city | `City` |

---

## Pipeline (4 notebooks, run in order)

| Notebook | Layer | What it does |
|---|---|---|
| `Bronze_Ecommerce.py` | Bronze | Raw CSV ingestion, `FullName` derivation, write as Delta |
| `Silver_Ecommerce.py` | Silver | Join orders + customers + weather, cleanse, enrich, validate |
| `Gold_Ecommerce.py` | Gold | 10 business aggregates incl. `gold_weather_sales` |
| `Fact_Tables.py` | Gold | 2 facts + 6 dimensions (`dim_weather` added) |

**Extract scripts:** `data_extraction.py` (orders), `weather_extraction.py` (weather API).

---

## Gold Output (served to Power BI)

**Aggregates (10):** `gold_orders`, `gold_tier`, `gold_revenue`, `gold_returns`, `gold_customer_segments`, `gold_season_category`, `gold_product_category`, `gold_payment_method`, `gold_device_type`, `gold_weather_sales`

**Facts (2):** `fact_data`, `fact_numerical`

**Dimensions (6):** `dim_customer`, `dim_product`, `dim_date`, `dim_payment`, `dim_device`, `dim_weather`

All registered in Unity Catalog: `ecommerce_catalog.gold.<table>`

---

## Dashboard

`customer360-retail-analytics Dashboards.pbix` — 4 pages: Overview, Sales Analysis, Customer Intelligence, Products and Returns. Connects to Databricks gold tables via the Databricks connector.

---

## Quick Start

1. Upload `Ecommerce_2000.csv` to ADLS `bronze/ecommerce/`
2. Create `dbo.customers` in Azure SQL; load records
3. Set `OPENWEATHER_API_KEY`, run `python weather_extraction.py`, upload `weather_data.csv` to Bronze
4. In Databricks: set `sql_server`/`sql_database` widgets + `azure-sql` secret scope
5. Run notebooks: Bronze → Silver → Gold → Fact_Tables
6. Open `.pbix` in Power BI, update Databricks connection, refresh

---

## Tech Stack

Azure Data Factory · Databricks · PySpark · Delta Lake · Unity Catalog · Power BI · OpenWeatherMap API

---

## Report

Full details: [`PROJECT_REPORT.md`](PROJECT_REPORT.md)
