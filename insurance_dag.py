# --------------------------------
# LIBRARIES
# --------------------------------

# Import Airflow Operators
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python import BranchPythonOperator
# Import Libraries to handle dataframes
import pandas as pd
import numpy as np
# Import Library to send Slack Messages
import requests
# Import Library to set DAG timing
from datetime import timedelta, datetime
import os
import re

# --------------------------------
# CONSTANTS
# --------------------------------

# Set input and output paths
FILE_PATH_INPUT = '/home/airflow/gcs/data/input/'
file_name = 'diag_metrics_P0015.csv'#os.listdir(FILE_PATH_INPUT)[0]
FILE_PATH_OUTPUT = '/home/airflow/gcs/data/output/'

# Import CSV into a pandas dataframe
df = pd.read_csv(FILE_PATH_INPUT+file_name)
# Slack webhook link
slack_webhook = 'https://hooks.slack.com/services/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'

# --------------------------------
# FUNCTION DEFINITIONS
# --------------------------------

# Function to send  messages over slack using the slack_webhook
def send_msg(text_string):
    """
    This function uses the requests library to send messages
    on Slack using a webhook
    """  
    requests.post(slack_webhook, json={'text': text_string}) 

# Function to generate the diagnostics report
# Add print statements to each variable so that it appears on the Logs
def send_report():

    sum_o2 = np.sum(df['o2_level'])
    sum_hr = np.sum(df['heart_rate'])
    avg_o2 = sum_o2/len(df['o2_level'])
    avg_hr = sum_hr/len(df['heart_rate'])
    std_o2 = np.std(df['o2_level'])
    std_hr = np.std(df['heart_rate'])
    min_o2 = np.min(df['o2_level'])
    min_hr = np.min(df['heart_rate'])
    max_o2 = np.max(df['o2_level'])
    max_hr = np.max(df['heart_rate'])
    
    
        
    body = """
    
    ----------------------------------------------------------
    Patient Details
    ----------------------------------------------------------
    #1. Average O2 level : {0}
    #2. Average Heart Rate : {1}
    #3. Standard Deviation of O2 level : {2}
    #4. Standard Deviation of Heart Rate : {3}
    #5. Minimum O2 level : {4}
    #6. Minimum Heart Rate : {5}
    #7. Maximum O2 level : {6}
    #8. Maximum Heart Rate : {7}
    """.format(avg_o2,avg_hr,std_o2,std_hr,min_o2,min_hr,max_o2,max_hr)
    
    send_msg(str(body))

#3 Function to filter anomalies in the data
# Add print statements to each output dataframe so that it appears on the Logs
def flag_anomaly():
    """
    As per the patient's past medical history, below are the mu and sigma 
    for normal levels of heart rate and o2:
    
    Heart Rate = (80.81, 10.28)
    O2 Level = (96.19, 1.69)

    Only consider the data points which exceed the (mu + 3*sigma) or (mu - 3*sigma) levels as anomalies
    Filter and save the anomalies as 2 dataframes in the output folder - hr_anomaly_P0015.csv & o2_anomaly_P0015.csv
    """
    o2filepath = '/home/airflow/gcs/data/output/o2_anomaly_P0015.csv'
    hrfilepath = '/home/airflow/gcs/data/output/hr_anomaly_P0015.csv'
    data_o2 = []
    data_hr = []
    if len(df) == 0: pass
    else:
        mu_o2 ,sigma_o2 = (96.19, 1.69)
        mu_hr,sigma_hr = (80.81, 10.28)
        
        for row in df['o2_level']:
            if row > (mu_o2 + 3*sigma_o2) or row > (mu_o2 - 3*sigma_o2):
                data_o2.append(row)
                
        for row in df['heart_rate']:
            if row > (mu_hr + 3*sigma_hr) or row > (mu_hr - 3*sigma_hr):
                data_hr.append(row)
                
                
    df_o2_anomaly = pd.DataFrame(data_o2)
    df_hr_anomaly = pd.DataFrame(data_hr)            
                
    df_o2_anomaly.to_csv(o2filepath, header=False, index=False)
    df_hr_anomaly.to_csv(hrfilepath, header=False, index=False)
# --------------------------------
# DAG definition
# --------------------------------

# Define the defualt args
default_args = {
    'owner' : 'Nidhi',
    'start_date' : datetime(2022,11,30),
    'depends_on_past': False,
    'retries':0
}
# Create the DAG object
with DAG(
    'insurance_dag',
    default_args = default_args,
    description = 'DAG',
    schedule_interval = None,
    catchup =False
) as dag:
    # start_task
    start_task = DummyOperator(
        task_id = 'start_task',
        dag=dag
    )
    # file_sensor_task
    file_sensor = FileSensor(
        task_id = 'file_sensor',
        poke_interval = 15,
        filepath = FILE_PATH_INPUT,
        timeout = 5,
        dag = dag
    )
    # send_report_task
    
    send_msg_task = PythonOperator(
        task_id = 'send_msg_task',
        python_callable = send_report,
        dag =dag
    
    )
    # flag_anomaly_task
    
    flag_anomaly_task = PythonOperator(
        task_id = 'flag_anomaly_task',
        python_callable = flag_anomaly,
        dag =dag
    )
    # end_task
    
    end_task = DummyOperator(
        task_id = 'end_task',
        dag=dag
    )
# Set the dependencies

start_task >> file_sensor
file_sensor >> send_msg_task >> end_task
file_sensor >> flag_anomaly_task >> end_task
# --------------------------------