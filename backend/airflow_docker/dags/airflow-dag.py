from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import sys
sys.path.append("..")
from app.etl import extract_races_from_fastf1, extract_circuit_from_fastf1, transform_race_results, transform_circuit_info, load_data_to_database

# def log(message):
#     timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second
#     now = datetime.now()
#     timestamp = now.strftime(timestamp_format)
#     with open("ETL_log.txt", 'a') as log: #os.path.join(LOGS_DIR, 
#         log.write(timestamp + ','+str(message)+'\n')

# Extract task function
def extract_task():
    print("Extract Executed")
    # log('Extract Started')
    # global df_extracted_race_results, df_extracted_circuit_info

    # df_extracted_race_results = extract_races_from_fastf1()
    # if df_extracted_race_results.empty:
    #     log('Latest Race Already Loaded')
    #     return False

    # df_extracted_circuit_info = extract_circuit_from_fastf1()
    # log('Extract Finished')
    # return True



# Transform task function
def transform_task():
    print("Transform Executed")
    # log('Transform Started')
    # global df_transformed_race_results, df_transformed_circuit_info

    # df_transformed_race_results = transform_race_results(df_extracted_race_results)
    # df_transformed_circuit_info = transform_circuit_info(df_extracted_circuit_info)
    # log('Transform Finished')


# Load task function
def load_task():
    print("Load Executed")
    # log('Load Started')
    # load_data_to_database(df_transformed_race_results, df_transformed_circuit_info)
    # log('Load Finished')


# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'f1_fantasy_etl',
    default_args=default_args,
    description='F1 Fantasy ETL',
    schedule_interval=timedelta(days=15),
    catchup=False,
)

# Define the tasks
t1 = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load',
    python_callable=load_task,
    dag=dag,
)

# Set task dependencies
t1 >> t2 >> t3
