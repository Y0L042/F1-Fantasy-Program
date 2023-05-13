# First stage: Base Python image
FROM python:3.10.9-slim as base

# Set a working directory
WORKDIR /app

# Copy your requirements file
COPY requirements.txt .

# Install the requirements
RUN pip install --trusted-host pypi.python.org -r requirements.txt



# Second stage: Airflow installation
FROM base as airflow

# Install Apache Airflow with the desired extras
RUN pip install apache-airflow[postgres]==2.6.0 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.0/constraints-3.8.txt"

# Set the AIRFLOW_HOME
ENV AIRFLOW_HOME=/airflow

# Copy your script into the dags directory
# COPY airflow.py ${AIRFLOW_HOME}/dags/

# Expose the port
EXPOSE 8080
