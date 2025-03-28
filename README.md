# Brazil's COVID-19 Data Pipeline with Airflow

This project sets up an automated pipeline to download, process, and analyze Brazil's COVID-19 data from the [Ministry of Health](https://opendatasus.saude.gov.br/), storing results in Google BigQuery for visualization.

## Data Report
 - [Looker Studio Report](https://lookerstudio.google.com/reporting/fb0a6b9d-99f8-413d-8095-392a4c01917c)

## Prerequisites

- Docker and Docker Compose installed
- Google Cloud Platform (GCP) account
  - JSON service account generated
- GCP Service Account credentials with permissions to:
  - BigQuery (Admin)
  - Cloud Storage (Admin)

## Setup Instructions

### 1. Configure Environment Variables

Create a `.env` file in the project root with these variables:

Generate the BASE64_JSON by running:
```bash
base64 -w0 /path/to/your/service-account.json > .creds
echo "BASE64_JSON=$(cat .creds)" >> .env
rm .creds
```

Verify the file creation and that the variable was populated.
```bash
BASE64_JSON=$(base64 -w0 /path/to/your/service-account.json)
```

### 2. Prepare GCP Resources

Ensure you have these GCP resources created:
- BigQuery dataset: `covid_dataset`
- Cloud Storage bucket: `covid_project_bucket`

### 3. Initialize the Environment

Build and start the containers:
```bash
docker-compose up -d
```

Wait a couple of minutes until Airflow fully initializes. You can always check logs with:
```bash
docker-compose logs -f airflow
```

### 4. Access Airflow Web UI

The Airflow UI will be available at:
```
http://localhost:8080
```
Credentials:
- Username: `admin`
- Password: `admin`

## Running the Pipeline

### Manual Execution

1. In the Airflow UI, find the `covid_pipeline_production` DAG
2. Click the "Play" button to trigger a manual run
3. Monitor progress in the DAG's Tree View

### Automated Execution

The pipeline is configured to run daily at midnight (UTC) automatically.

## Pipeline Steps

1. **Download and Extract**: Fetches the latest COVID-19 data from the Ministry of Health
2. **Upload to GCS**: Stores raw data in Google Cloud Storage
3. **Load to BigQuery**: Imports data into BigQuery's raw table
4. **Transform**: Processes data to create analytical tables with:
   - Country-wide cumulative totals
   - Regional daily breakdowns
   - Mortality rate calculations

## Verifying Results

After successful execution, check these BigQuery tables:
- Raw data: `covid_dataset.raw_covid_data`
- Transformed data: `covid_dataset.transformed_data`


### This project was done as a part of the [Data Engineering Zoomcamp Course](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)