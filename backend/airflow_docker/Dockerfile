FROM apache/airflow:2.6.0

# slim-2.6.1rc2-python3.10

# COPY dags /usr/local/airflow/dags
COPY app /usr/local/airflow/app
COPY requirements.txt /requirements.txt



# Install fastf1 and other requirements
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
# RUN pip install fastf1
