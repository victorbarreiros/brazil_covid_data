FROM apache/airflow:2.7.3

USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

USER airflow
RUN pip install --user \
  google-cloud-storage \
  google-cloud-bigquery \
  google-cloud-dataproc \
  google-auth \
  google-auth-oauthlib \
  apache-airflow-providers-google \
  pyspark \
  oauth2client
