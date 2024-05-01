from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def extract_data(date1):
    import time
    import os
    import json
    import requests
    from datetime import datetime, timedelta

    # hitting the api and getting the data for the location_id and parameter
    def save_data(parameter, location_id, tries):
        print()
        url = f"https://api.openaq.org/v2/measurements?location_id={location_id}&parameter={parameter}&date_from={date1}&date_to={date2}&limit=1000"
        print(url)
        response = requests.get(url)
        if response.status_code == 200: # successful hit
            print(f'Hit Success on location_id = {location_id} parameter = {parameter}')
            # Access the response data in JSON format
            data = response.json()
            # check if you've got some data or not
            if len(data['results']) > 0:
                with open(
                        f"/home/sad7_5407/Desktop/Data_Engineering/data/{yr}/{mnt}/{dy}/{location_id}/{parameter}.json",
                        'w') as file:
                    json.dump(data['results'], file, indent=2)
                    print(f'data/{yr}/{mnt}/{dy}/{location_id}/{parameter}.json saved locally....')
            else:
                print(f' -> -> Nothing on {url}')
            time.sleep(1)
        else: # hit error
            print(
                f" >>>>>>>> Error in hit on location_id = {location_id} parameter = {parameter} tries = {tries} : {response.status_code}")
            if tries < 3:
                time.sleep(31 + 10 * tries)
                save_data(parameter, location_id, tries + 1)

    location_parameters = {
        8118: ["pm25"],  # New Delhi
        8172: ['pm25'],  # Kolkata
        8039: ['pm25'],  # Mumbai
        8557: ['pm25'],  # Hyderabad
        8558: ['pm25']  # Chennai
    }

    # get the execution date of the DAG
    print(f'date1 = {date1}')
    date1 = date1[0:10]
    date1 = datetime.strptime(date1, '%Y-%m-%d').date() # dag execution date
    date2 = date1 + timedelta(days=1) #next_day

    date1 = str(date1)[:10]
    date2 = str(date2)[:10]


    dy = date1[-2:]
    mnt = date1[5:7]
    yr = date1[:4]

    call = 0
    for location_id in location_parameters.keys():
        # make the directory of location_id
        os.makedirs(f'/home/sad7_5407/Desktop/Data_Engineering/data/{yr}/{mnt}/{dy}/{location_id}', exist_ok=True)
        for parameter in location_parameters[location_id]:
            save_data(parameter, location_id, 0)
            call += 1
            if call % 3 == 0:
                time.sleep(6)
            print()


dag_arg = {
    'owner': 'Rahul Chaurasiya',
    'retries': '3',
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='OpenAQ_v9_3',
        default_args=dag_arg,
        schedule_interval='@daily',
        start_date=datetime(2024, 1, 25),
        catchup=True,
        max_active_runs=1
) as dag:

    extract = PythonOperator(
        task_id='extract_data_through_api',
        python_callable=extract_data,
        op_args=[
            "{{ dag_run.execution_date }}"
        ]
    )

    transform_load = SparkSubmitOperator(
        task_id='transform_load',
        application='/home/sad7_5407/airflow/include/OpenAQ_extract_transform.py',
        conn_id='spark_submit2',
        application_args=[
            ' {{ dag_run.execution_date }} '
        ]
    )

    delete_local_data = BashOperator(
        task_id='delete_local_data',
        # bash_command='rm -r /home/sad7_5407/Desktop/Data_Engineering/data/*'
        bash_command='echo uncomment to delete_local_data ....'
    )

    wait_for_safemode = BashOperator(
        task_id='wait_for_safemode',
        bash_command="""#! /bin/bash

        check_safemode(){
            hdfs dfsadmin -safemode get | grep "Safe mode is ON"
        }

        while check_safemode; do
            echo "Waiting for NameNode to Come Out of SafeMode. Sleeping for 10 seconds........"
            sleep 10
        done

        echo "NameNode come out of SafeMode.."
        """
    )

    build_latest_table = SparkSubmitOperator(
        task_id='build_latest_table',
        application='/home/sad7_5407/airflow/include/OpenAQ_build_latest.py',
        conn_id='spark_submit2'
    )

extract >> transform_load >> delete_local_data >> wait_for_safemode >> build_latest_table
