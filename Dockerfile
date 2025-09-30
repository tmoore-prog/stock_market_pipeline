FROM apache/airflow:3.0.6-python3.12
USER airflow
COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install apache-airflow-providers-databricks
RUN pip install --upgrade --force-reinstall protobuf==6.32.1

ENV PYTHONPATH="/opt/airflow/src"

