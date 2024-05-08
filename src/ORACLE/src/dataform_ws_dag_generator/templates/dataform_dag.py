from __future__ import annotations
import os,sys
from datetime import datetime
import logging
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.utils.dataform import make_initialization_workspace_flow
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateRepositoryOperator,
    DataformCreateWorkspaceOperator,
    DataformInstallNpmPackagesOperator,
    DataformMakeDirectoryOperator,
    DataformWriteFileOperator,
)
import google.auth
from google.cloud import storage

# *********  configuration variables *********

PROJECT_ID = "${project_id}"
REGION = "${dataform_ws_region}"
DAG_ID = f"oracle_dataform_ws_demo"
REPOSITORY_ID = "${repository_id}"
WORKSPACE_ID = "${workspace_id}"
DATAFORM_SCHEMA_NAME = f"schema_{DAG_ID}"
BUCKET_NAME = "${bucket_name}"

def list_files_in_bucket(bucket_name):
    client = storage.Client()
    blobs = list(client.list_blobs(bucket_name))
    file_names = [blob.name for blob in blobs if blob.name.endswith((".sqlx", ".js"))]
    logging.info(f"List of files in bucket: {file_names}")
    return file_names

def read_file_from_gcs(bucket_name, file_name):
    credentials, _= google.auth.default()
    client = storage.Client(credentials=credentials)
    BUCKET = client.get_bucket(bucket_name)
    blob = BUCKET.blob(file_name)
    assert blob.exists()
    data = blob.download_as_string().decode('utf-8')
    return data

def write_to_dataform(file_name, data, directory, **kwargs):
    file_content = data.encode('utf-8')
    logging.info(f"Writing file '{file_name}' to '{directory}' in Dataform")
    if directory == "OracleFinance":
        filepath = f"definitions/{directory}/Edw/{file_name}"
    elif directory == "sources":
        filepath = f"definitions/sources/{file_name}"
    else:
        raise ValueError(f"Invalid directory: {directory}")
    
    write_file = DataformWriteFileOperator(
        task_id=f"write_to_dataform_{file_name}",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        filepath=filepath,
        contents=file_content,
    )
    return write_file.execute(context=kwargs)

def read_and_write_to_dataform(**context):
    file_names = context['task_instance'].xcom_pull(task_ids='list_files')
    for file_name in file_names:
        directory = "OracleFinance" if file_name.endswith(".sqlx") else "sources"
        data = read_file_from_gcs(BUCKET_NAME, file_name)
        write_to_dataform(file_name, data, directory)

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["oracle", "dataform"],
) as dag:
    
    make_repository = DataformCreateRepositoryOperator(
        task_id="make-repository",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
    )

    make_workspace = DataformCreateWorkspaceOperator(
        task_id="make-workspace",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
    )

    first_initialization_step, last_initialization_step = make_initialization_workspace_flow(
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        package_name=f"dataform_package",
        without_installation=True,
        dataform_schema_name=DATAFORM_SCHEMA_NAME,
    )
    
    install_npm_packages = DataformInstallNpmPackagesOperator(
        task_id="install-npm-packages",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
    )

    make_oracle_directory = DataformMakeDirectoryOperator(
        task_id="make-oracle-directory",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        directory_path="definitions/OracleFinance",
    )

    make_source_directory = DataformMakeDirectoryOperator(
        task_id="make-source-directory",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        directory_path="definitions/sources",
    )

    list_files_task = PythonOperator(
        task_id='list_files',
        python_callable=list_files_in_bucket,
        op_kwargs={'bucket_name': BUCKET_NAME},
        provide_context=True,
    )

    read_write_task = PythonOperator(
        task_id='read_write_task',
        python_callable=read_and_write_to_dataform,
        provide_context=True,
    )

    make_repository >> make_workspace >> first_initialization_step, last_initialization_step >> install_npm_packages >> make_oracle_directory >> make_source_directory >>list_files_task >> read_write_task
