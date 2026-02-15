# infotennis_v2

A cloud-native tennis data pipeline using Prefect, AWS S3, and MotherDuck.

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌──────────────────┐
│  ATP Website    │────▶│    AWS S3    │────▶│   MotherDuck     │
│  (Data Source)  │     │  (Data Lake) │     │ (Data Warehouse) │
└─────────────────┘     └──────────────┘     └──────────────────┘
         │                      │                      │
         └──────────────────────┴──────────────────────┘
                         Prefect (Orchestration)
```

## Quick Start

### 1. Clone and Install Dependencies

```bash
cd infotennis_v2
uv sync  # or pip install -r requirements.txt
```

### 2. Configure Credentials

Copy the example environment file and fill in your credentials:

```bash
cp config/.env.example .env
```

Edit `.env` with your actual credentials:

```ini
# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
AWS_REGION=us-east-1
S3_BUCKET=infotennis-v2

# MotherDuck Configuration
MOTHERDUCK_TOKEN=your-motherduck-token
MOTHERDUCK_DATABASE=infotennis_raw
```

### 3. Run the Pipeline

```bash
# Run for current year
python flows/atp_main_flow.py

# Run for a specific year
python flows/atp_main_flow.py --year 2024

# Run for multiple years
python flows/atp_main_flow.py --start-year 2020 --end-year 2024
```

## Project Structure

```
infotennis_v2/
├── config/
│   ├── __init__.py
│   ├── config.py          # Centralized configuration loader
│   ├── config.yaml        # Non-sensitive settings
│   └── .env.example       # Credential template
├── flows/
│   └── atp_main_flow.py   # Prefect flow orchestration
├── tasks/
│   ├── __init__.py
│   ├── scrape_atp_calendar.py  # ATP scraper task
│   ├── s3_storage.py           # S3 upload task
│   └── motherduck_load.py      # MotherDuck loader task
├── .env                   # Your credentials (gitignored)
├── pyproject.toml
└── README.md
```

## Configuration

### Environment Variables (`.env`)

| Variable | Description | Required |
|----------|-------------|----------|
| `AWS_ACCESS_KEY_ID` | AWS IAM access key | ✅ |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM secret key | ✅ |
| `AWS_REGION` | AWS region (default: us-east-1) | ❌ |
| `S3_BUCKET` | S3 bucket name | ✅ |
| `MOTHERDUCK_TOKEN` | MotherDuck API token | ✅ |
| `MOTHERDUCK_DATABASE` | MotherDuck database name | ❌ |

### S3 Naming Convention

Files are stored with Hive-style partitioning:

```
s3://{bucket}/raw/{endpoint_name}/year={YYYY}/month={MM}/{timestamp}.json
```

Example:
```
s3://infotennis-v2/raw/atp_results_archive/year=2024/month=02/20240205_235647.json
```

## MotherDuck Tables

### `raw_atp_results_archive`

Contains tournament data with automatic schema inference:

| Column | Type | Description |
|--------|------|-------------|
| `tournaments` | JSON | Array of tournament records |
| `metadata` | JSON | Scrape metadata |
| `_source_file` | VARCHAR | S3 URI of source file |
| `_loaded_at` | TIMESTAMP | When the data was loaded |

### `_loaded_files`

Metadata table for idempotency tracking:

| Column | Type | Description |
|--------|------|-------------|
| `s3_uri` | VARCHAR (PK) | S3 URI of loaded file |
| `endpoint` | VARCHAR | Endpoint name |
| `table_name` | VARCHAR | Target table |
| `rows_loaded` | INTEGER | Number of rows |
| `loaded_at` | TIMESTAMP | Load timestamp |

## Querying Data in MotherDuck

```sql
-- View recent loads
SELECT * FROM _loaded_files ORDER BY loaded_at DESC LIMIT 10;

-- Query tournament data
SELECT 
    t.tournament,
    t.city,
    t.country,
    t.category,
    t.singles_winner
FROM raw_atp_results_archive, 
     UNNEST(tournaments) AS t
WHERE metadata.year = 2024;

-- Count tournaments by category
SELECT 
    t.category,
    COUNT(*) as tournament_count
FROM raw_atp_results_archive,
     UNNEST(tournaments) AS t
GROUP BY t.category;
```

## Adding New Endpoints

The pipeline is designed to be modular. To add a new data endpoint:

1. Create a new scraper task in `tasks/`:
   ```python
   @task
   def scrape_new_endpoint_task() -> list:
       # Your scraping logic
       return data
   ```

2. Create a new flow in `flows/`:
   ```python
   @flow
   def new_endpoint_flow():
       data = scrape_new_endpoint_task()
       s3_uri = upload_to_s3(data={"records": data}, endpoint_name="new_endpoint")
       rows = load_to_motherduck(s3_uri=s3_uri)
       return {"s3_uri": s3_uri, "rows_loaded": rows}
   ```

## Prefect Deployment

To deploy to Prefect Cloud for scheduled runs:

```bash
# Login to Prefect Cloud
prefect cloud login

# Deploy the flow
prefect deploy flows/atp_main_flow.py:atp_results_archive_flow \
    --name "ATP Daily Pipeline" \
    --cron "0 6 * * *"  # Run daily at 6 AM
```

## Troubleshooting

### Common Issues

1. **Missing credentials error**: Ensure `.env` file exists in project root with all required variables.

2. **S3 access denied**: Check AWS IAM permissions include `s3:PutObject` and `s3:GetObject`.

3. **MotherDuck connection failed**: Verify your token at https://app.motherduck.com/settings/tokens.

4. **Duplicate data**: The pipeline uses `_loaded_files` table for idempotency. Check this table if you suspect issues.

### Logs

Flow execution logs are available in:
- Console output during local runs
- Prefect UI when deployed to Prefect Cloud

## License

MIT License
