from Snowflake_remove_old_staged_files import Remove_old_staged_files
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from Snowflake_upload_files import Upload_files_to_stage


default_args = {
    'start_date': datetime(year=2021, month=5, day=12)
}


def remove_files():
    remove = Remove_old_staged_files("cred.json")
    remove.delete_old_staged_files()


def upload_files():
    upload = Upload_files_to_stage("cred.json")
    upload.run_upload()


with DAG(
        dag_id='Snowflake_poc',
        default_args=default_args,
        schedule_interval='@daily',
        description='ETL pipeline for processing users'
) as dag:

    remove_old_stage_files = PythonOperator(
        task_id='remove_old_stage_files',
        python_callable=remove_files,
    )

    upload_new_files = PythonOperator(
        task_id='upload_new_files',
        python_callable=upload_files
    )

    remove_old_stage_files >> upload_new_files
