# Customer360 Retail Analytics — Project Report

**Course:** Data Engineering Industry Project  
**Deadline:** 20 November 2026  
**Stack:** Azure Data Factory · Databricks · PySpark · Delta Lake · Unity Catalog · Power BI  
**Repository:** `https://github.com/piyush080306-pixel/customer360-retail-analytics`

---

## 1. Problem Statement

E-commerce data is scattered across order exports, customer databases, payment logs, and external services (e.g., weather). Each source has its own identifiers, formats, and refresh cadence, which makes cross-source reporting slow and error-prone.

This project solves that by building a governed **Customer 360** analytical model that unifies:

- E-commerce order data (CSV)
- Customer master records (Azure SQL Database)
- Weather data by city (OpenWeatherMap API — third data source)

The result is a medallion-architecture pipeline (Bronze → Silver → Gold) serving a Power BI dashboard for revenue, customer value, product performance, payment behaviour, device usage, returns, and weather-impact analysis.

---

## 2. Architecture

The pipeline follows the **Medallion Architecture** pattern:

```
Sources                          Azure Data Lake Storage Gen2
  │                                  ┌─────────────┐
  │   CSV orders                     │  Bronze     │
  │   Azure SQL customers            │  (Raw CSV   │
  │   OpenWeatherMap API (weather)   │   + Delta)  │
  └───────────┬──────────────────────└──────┬──────┘
              │                             │
              ▼                             ▼
    Azure Databricks (PySpark)      ┌─────────────┐
    - Bronze: raw ingestion         │  Silver     │
    - Silver: cleanse, join, enrich │  (Cleaned   │
    - Gold: aggregates, facts, dims │   Delta)    │
    └───────────┬────────────────────└──────┬──────┘
                │                             │
                ▼                             ▼
        Unity Catalog (Governed Tables) ◄───┘
                │
                ▼
         Power BI Dashboard (4 pages)
                │
                ▼
        Self-service analytics
```

**Orchestration:** Azure Data Factory runs the four Databricks notebooks in sequence:
1. `Bronze_Ecommerce.py`
2. `Silver_Ecommerce.py`
3. `Gold_Ecommerce.py`
4. `Fact_Tables.py`

Trigger: Daily at 08:00 IST (disabled in development to control costs).

---

## 3. Data Sources

| # | Source | Type | Records | Access Method | Purpose |
|---|--------|------|---------|---------------|---------|
| 1 | `Ecommerce_2000.csv` | Public dataset / file | 2,000 orders, 23 attributes | File upload → ADLS Bronze | Core order transactions |
| 2 | `dbo.customers` | Relational | Customer master | JDBC from Databricks | Customer identity & location |
| 3 | OpenWeatherMap API | REST API | Weather per city | `weather_extraction.py` script | Weather enrichment (3rd source) |

### 3.1 E-Commerce Orders CSV
23 columns including `OrderID`, `CustomerID`, `OrderDate`, `ProductCategory`, `UnitPrice`, `Quantity`, `TotalAmount`, `DiscountPercent`, `FinalAmount`, `PaymentMethod`, `DeviceType`, `MembershipTier`, `ShippingDays`, `Returned`, `ReviewScore`, and more.

### 3.2 Azure SQL Database — Customers
Schema:
```sql
CREATE TABLE dbo.customers (
    customer_id   VARCHAR(10),
    customer_name VARCHAR(100),
    city          VARCHAR(50)
);
```
Joined to orders on `CustomerID`.

### 3.3 Weather Data (OpenWeatherMap API)
Extracted by `weather_extraction.py`. For each unique city in the orders dataset, fetches:
- `Weather_Temp_C` — temperature (Celsius)
- `Weather_Humidity` — humidity (%)
- `Weather_Condition` — condition (e.g., Clear, Rain, Clouds)
- `Weather_Wind_Speed` — wind speed (m/s)

Stored as CSV in Bronze, then joined to Silver on `City`.

---

## 4. Pipeline Layers

### Layer 1 — Data Extraction (`Bronze`)
- **Notebook:** `Bronze_Ecommerce.py`
- Reads `Ecommerce_2000.csv` from ADLS Bronze container.
- Derives `FullName = FirstName + " " + LastName`.
- Writes raw data as **Delta** to Bronze (transactional, time-travel capable).
- Weather data extracted separately via `weather_extraction.py` and staged in Bronze.

### Layer 2 — Data Engineering (`Silver`)
- **Notebook:** `Silver_Ecommerce.py`
- Reads Bronze orders (Delta) and Azure SQL customers (JDBC).
- Left-joins orders to customers on `CustomerID` (business key).
- Left-joins weather data on `City` (third source).
- Adds derived columns:
  - `IsDiscounted` (Yes/No)
  - `Weekend` / `Weekday`
  - `Value_Orders` (High / Medium / Low)
  - `Order_Year`, `Order_Month`, `Order_Quarter`
- **Data quality validation:** row counts, matched/unmatched customers, null OrderID, invalid Quantity, negative FinalAmount, missing weather records.
- Writes cleaned Delta to Silver, registered as `ecommerce_catalog.silver.orders_silver`.

### Layer 3 — Analytics Serving (`Gold`)
- **Notebooks:** `Gold_Ecommerce.py`, `Fact_Tables.py`
- **Aggregates (Gold_Ecommerce.py):** 9 core tables + 1 weather table:
  - `gold_orders` — orders per customer/city
  - `gold_tier` — customers by membership tier
  - `gold_revenue` — revenue by city
  - `gold_returns` — orders by return status
  - `gold_customer_segments` — LTV-based segmentation (Platinum/Gold/Silver/Bronze)
  - `gold_season_category` — orders/revenue by season × category
  - `gold_product_category` — orders/revenue by product category
  - `gold_payment_method` — orders/revenue/AOV by payment method
  - `gold_device_type` — orders/revenue/AOV by device type
  - `gold_weather_sales` — orders/revenue/AOV by weather condition (NEW)
- **Facts & Dimensions (Fact_Tables.py):**
  - `fact_data` — order-level descriptive facts
  - `fact_numerical` — order-level measures
  - `dim_customer`, `dim_product`, `dim_date`, `dim_payment`, `dim_device`, `dim_weather` (NEW)
- All tables registered in Unity Catalog under `ecommerce_catalog.gold`.

---

## 5. Transformations & Data Quality

| Transformation | Layer | Logic |
|---|---|---|
| Full-name derivation | Bronze | `FirstName + " " + LastName` |
| Multi-source join | Silver | Orders ⋈ Customers (`CustomerID`) ⋈ Weather (`City`) |
| Duplicate-column removal | Silver | Drop duplicate `city` from customer table |
| Discount flag | Silver | `IsDiscounted = Yes` when `DiscountPercent > 0` |
| Weekend flag | Silver | `Weekend` if Saturday/Sunday, else `Weekday` |
| Order value band | Silver | High (>1000), Medium (500–1000), Low (≤500) |
| Date decomposition | Silver | `Order_Year`, `Order_Month`, `Order_Quarter` |
| Customer segmentation | Gold | LTV: Platinum ≥10,000 / Gold ≥5,000 / Silver ≥1,000 / Bronze |
| Weather enrichment | Silver | Join OpenWeatherMap data on `City` |
| Dimensional modelling | Gold | 2 fact tables + 6 dimension tables with surrogate keys |

### Data Quality Checks (Silver output)
- Total Silver row count
- Matched vs. unmatched customer count
- Null `OrderID` count
- Invalid `Quantity` (≤0) count
- Negative `FinalAmount` count
- Missing weather data count

---

## 6. Power BI Dashboard

**File:** `customer360-retail-analytics Dashboards.pbix`

Connects to Databricks (Unity Catalog gold tables) via the Databricks connector.

| Page | Focus | Key Visuals |
|---|---|---|
| Overview | Executive summary | Total revenue, total orders, avg LTV, avg order value |
| Sales Analysis | Revenue drivers | Revenue by city, payment method, season, day of week |
| Customer Intelligence | Customer value | Segments, membership tier, device type, LTV by tier |
| Products and Returns | Product performance | Revenue/returns by category, seasonal trends, quarterly orders |

*Weather-based insights (from `gold_weather_sales`) can be added to the Sales Analysis or a new page to show revenue by weather condition.*

---

## 7. Tech Stack

| Domain | Technology |
|---|---|
| Cloud platform | Microsoft Azure |
| Orchestration | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 (`abfss://`) |
| Relational source | Azure SQL Database |
| Third source API | OpenWeatherMap |
| Compute | Azure Databricks |
| Processing | PySpark, Spark SQL |
| Table format | Delta Lake |
| Governance / catalog | Unity Catalog (`ecommerce_catalog`) |
| Visualization | Power BI Desktop |
| Secrets | Databricks Secrets (`azure-sql` scope) |

---

## 8. Getting Started

### Prerequisites
- Azure subscription with ADF, ADLS Gen2, Azure SQL Database, Databricks.
- Databricks cluster with PySpark + Delta Lake + JDBC driver.
- Unity Catalog enabled with `ecommerce_catalog`.
- Power BI Desktop.
- OpenWeatherMap API key (for weather data — optional; pipeline runs without it).

### Setup Steps
1. **Provision Azure resources** — storage account (containers: `bronze`, `silver`, `gold`), SQL server/database, Data Factory, Databricks workspace.
2. **Stage source data** — upload `Ecommerce_2000.csv` to Bronze container under `ecommerce/`. Create `dbo.customers` in Azure SQL and load records.
3. **Extract weather data** — set `OPENWEATHER_API_KEY` and run:
   ```bash
   python weather_extraction.py
   ```
   Upload the generated `weather_data.csv` to Bronze under `ecommerce/weather_data`.
4. **Configure Databricks** — set `sql_server`, `sql_database` widgets; create `azure-sql` secret scope with `username`/`password`; replace `stdataproject` with your storage account name in notebook paths.
5. **Register Unity Catalog** — ensure `ecommerce_catalog`, `silver`, and `gold` schemas exist.
6. **Run pipeline in order:**
   ```
   Bronze_Ecommerce.py
   Silver_Ecommerce.py
   Gold_Ecommerce.py
   Fact_Tables.py
   ```
7. **Connect Power BI** — open the `.pbix`, update Databricks connection (hostname, HTTP path, access token), refresh.

---

## 9. Security Notes

- Credentials stored in Databricks Secrets scope `azure-sql`; never committed to the repository.
- OpenWeatherMap API key passed via environment variable, not hardcoded.
- ADLS access restricted via Unity Catalog external locations and least-privilege accounts.
- `.gitignore` excludes local secrets, checkpoints, and IDE files.

---

## 10. Deliverables Checklist

| Deliverable | Status |
|---|---|
| Project title & problem statement | ✅ |
| Data source details + extraction script | ✅ (`weather_extraction.py`) |
| Raw data storage implementation | ✅ (Bronze Delta) |
| Databricks notebooks (Bronze, Silver, Gold) | ✅ |
| Data cleaning, transformation, validation | ✅ |
| Complete ETL pipeline | ✅ |
| Power BI dashboard connected to Gold | ✅ |
| Project architecture diagram | ✅ (Section 2) |
| GitHub repository with README | ✅ |
| Project report | ✅ (this document) |

---

*Prepared for the Data Engineering Industry Project — E-Commerce: Customer Behaviour and Sales Intelligence.*
