from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery, storage
from datetime import datetime
import os, base64, json, requests, zipfile

BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'covid_project_bucket')
BQ_DATASET_PATH = os.getenv('BQ_DATASET_PATH', 'datawarehousing-de-zoomcamp.covid_dataset')

def get_credentials():
    if encoded_json := os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'):
        return json.loads(base64.b64decode(encoded_json))

    if encoded_json := Variable.get("GCP_CREDENTIALS_JSON", default_var=None):
        return json.loads(base64.b64decode(encoded_json))

    raise ValueError("No credentials found in environment or Airflow variables")

def download_and_extract():
    url = "https://mobileapps.saude.gov.br/esus-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/f56f67c7009cbf89282218e6b52d46c6_HIST_PAINEL_COVIDBR_14mar2025.zip"
    os.makedirs('data/extracted', exist_ok=True)

    response = requests.get(url)
    response.raise_for_status()

    with open('data/covid.zip', 'wb') as f:
        f.write(response.content)

    with zipfile.ZipFile('data/covid.zip', 'r') as zip_ref:
        zip_ref.extractall('data/extracted')

    os.remove('data/covid.zip')

def upload_to_gcs():
    creds = get_credentials()
    client = storage.Client.from_service_account_info(creds)

    bucket = client.bucket(BUCKET_NAME)
    for root, _, files in os.walk('data/extracted'):
        for file in files:
            local_path = os.path.join(root, file)
            blob = bucket.blob(f"raw/{file}")
            blob.upload_from_filename(local_path)
            print(f"Uploaded {file} to GCS")

def load_to_bigquery():
    creds = get_credentials()
    client = bigquery.Client.from_service_account_info(creds)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("regiao", "STRING"),
            bigquery.SchemaField("estado", "STRING"),
            bigquery.SchemaField("municipio", "STRING"),
            bigquery.SchemaField("coduf", "INTEGER"),
            bigquery.SchemaField("codmun", "INTEGER"),
            bigquery.SchemaField("codRegiaoSaude", "INTEGER"),
            bigquery.SchemaField("nomeRegiaoSaude", "STRING"),
            bigquery.SchemaField("data", "DATE"),
            bigquery.SchemaField("semanaEpi", "INTEGER"),
            bigquery.SchemaField("populacaoTCU2019", "INTEGER"),
            bigquery.SchemaField("casosAcumulado", "INTEGER"),
            bigquery.SchemaField("casosNovos", "INTEGER"),
            bigquery.SchemaField("obitosAcumulado", "INTEGER"),
            bigquery.SchemaField("obitosNovos", "INTEGER"),
            bigquery.SchemaField("Recuperadosnovos", "INTEGER"),
            bigquery.SchemaField("emAcompanhamentoNovos", "INTEGER"),
            bigquery.SchemaField("interior_metropolitana", "STRING"),
        ],
        skip_leading_rows=1,
        source_format='CSV',
        field_delimiter=';',
        write_disposition='WRITE_TRUNCATE',
        max_bad_records=10,
        ignore_unknown_values=True,
        allow_quoted_newlines=True,
        allow_jagged_rows=True
    )

    load_job = client.load_table_from_uri(
        f"gs://{BUCKET_NAME}/raw/HIST_PAINEL_COVIDBR_*.csv",
        f"{BQ_DATASET_PATH}.raw_covid_data",
        job_config=job_config
    )
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows")

def transform_with_bigquery():
    creds = get_credentials()
    client = bigquery.Client.from_service_account_info(creds)

    query = f"""
    CREATE OR REPLACE TABLE `{BQ_DATASET_PATH}.transformed_data` AS
    WITH
    -- Get country-wide totals (using Brasil/coduf=76 records)
    country_data AS (
      SELECT
        DATE(data) as date,
        CAST(casosNovos AS INT64) AS country_cases,
        CAST(obitosNovos AS INT64) AS country_deaths
      FROM `{BQ_DATASET_PATH}.raw_data`
      WHERE regiao = 'Brasil' AND coduf = 76
    ),

    -- Get regional breakdowns (excluding country-wide records)
    regional_data AS (
      SELECT
        regiao,
        estado,
        DATE(data) as date,
        CAST(casosNovos AS INT64) AS casosNovos,
        CAST(obitosNovos AS INT64) AS obitosNovos
      FROM `{BQ_DATASET_PATH}.raw_data`
      WHERE
        estado IS NOT NULL
        AND estado != ''
        AND municipio IS NOT NULL
        AND municipio != ''
    ),

    -- Calculate country-wide cumulative totals
    country_cumulative AS (
      SELECT
        date,
        country_cases,
        country_deaths,
        SUM(country_cases) OVER (ORDER BY date) AS cumulative_cases_country,
        SUM(country_deaths) OVER (ORDER BY date) AS cumulative_deaths_country
      FROM country_data
    ),

    -- Calculate regional daily totals
    regional_daily AS (
      SELECT
        regiao,
        estado,
        date,
        SUM(casosNovos) AS daily_cases_region,
        SUM(obitosNovos) AS daily_deaths_region
      FROM regional_data
      GROUP BY regiao, estado, date
    )

    -- Combine both datasets
    SELECT
      c.date,
      -- Country-wide metrics
      c.country_cases AS daily_cases_country,
      c.country_deaths AS daily_deaths_country,
      c.cumulative_cases_country,
      c.cumulative_deaths_country,
      ROUND((c.cumulative_deaths_country / NULLIF(c.cumulative_cases_country, 0)) * 100, 2) AS mortality_rate_country,
      -- Regional metrics
      r.regiao,
      r.estado,
      r.daily_cases_region,
      r.daily_deaths_region,
      -- Date parts
      EXTRACT(YEAR FROM c.date) AS year,
      EXTRACT(MONTH FROM c.date) AS month,
      EXTRACT(DAY FROM c.date) AS day
    FROM country_cumulative c
    LEFT JOIN regional_daily r ON c.date = r.date
    ORDER BY c.date, r.regiao, r.estado
    """

    try:
        job = client.query(query)
        job.result()
        print("Transformation completed successfully")

        verification_query = f"""
        SELECT
          COUNT(DISTINCT date) AS days_processed,
          COUNT(DISTINCT estado) AS states_processed,
          SUM(daily_cases_country) AS total_country_cases,
          SUM(daily_deaths_country) AS total_country_deaths
        FROM `{BQ_DATASET_PATH}.transformed_data`
        """

        verification_job = client.query(verification_query)
        results = list(verification_job.result())[0]

        print("\nVerification Results:")
        print(f"- Days processed: {results.days_processed}")
        print(f"- States processed: {results.states_processed}")
        print(f"- Total country cases: {results.total_country_cases:,}")
        print(f"- Total country deaths: {results.total_country_deaths:,}")

    except Exception as e:
        print(f"Transformation failed: {str(e)}")
        raise

with DAG(
    'covid_pipeline_production',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'retries': 2
    },
) as dag:

    download_task = PythonOperator(
        task_id='download_and_extract',
        python_callable=download_and_extract,
    )

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
    )

    load_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
    )

    transform_task = PythonOperator(
        task_id='transform_with_bigquery',
        python_callable=transform_with_bigquery,
        dag=dag
    )

    download_task >> upload_task >> load_task >> transform_task
