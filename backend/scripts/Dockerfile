FROM python:3.10.9-slim

ENV AIRFLOW_HOME=/airflow
ENV PYTHONUNBUFFERED=1
ENV PATH=$PATH:$AIRFLOW_HOME/bin

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt --user

ENV PATH="/root/.local/bin:$PATH"

COPY airflow-server/dags/ ${AIRFLOW_HOME}/dags/
COPY etl.py /app/etl.py

EXPOSE 8080

CMD ["airflow", "webserver", "--host", "0.0.0.0"]
