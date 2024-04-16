
from __future__ import annotations
import os,sys
from datetime import datetime
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
#Get the dag filepath
filePath=os.path.abspath(__file__)

#Include the Utilities Folder path in the system path 
utilityDirName="CommonPattern"
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(filePath)))+os.sep+utilityDirName)

#Import the utility and config libraries

import config , utils, runaudit
from config import Config
from utils import Utils

configObj = Config("config_init")
utilsObj = Utils("utils_init")
# Path to the configurations Directory
configurationsDirPath = configObj.getConfigurationsPath(filePath)

# Absolute path configurations Variable File
configVariablePath = configObj.getConfigVariablePath(configurationsDirPath)

# Read all the configuration variables 
config_variables = utilsObj.read_json(configVariablePath)


# *********  configuration variables *********

PROJECT_ID = config_variables['Project']
REGION = config_variables['DF_Region']
DAG_ID = "oracle_dataform_dag"
REPOSITORY_ID = config_variables['Repository_Id']
WORKSPACE_ID = config_variables['Workspace_Id']
DATAFORM_SCHEMA_NAME = f"schema_{DAG_ID}"
BUCKET_NAME = config_variables['Target_Bucket']


def list_files_in_bucket(bucket_name):
    credentials, _ = google.auth.default()
    client = storage.Client(credentials=credentials)
    bucket = client.get_bucket(bucket_name)

    blobs = list(client.list_blobs(bucket))
    file_names = [blob.name for blob in blobs if blob.name.endswith(".sqlx")]
    return file_names

def write_to_dataform(dataset_file_name, data, **kwargs):
    file_content = data.encode('utf-8')
    write_test_file = DataformWriteFileOperator(
        task_id=f"write_to_dataform_{dataset_file_name}",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        filepath=f'definitions/OracleFinance/{dataset_file_name}',
        contents=file_content,
    )
    return write_test_file.execute(context=kwargs)

def read_file_from_gcs(bucket_name, file_name):
    credentials, _= google.auth.default()
    client = storage.Client(credentials=credentials)
    BUCKET = client.get_bucket(bucket_name)
    blob = BUCKET.blob(file_name)
    assert blob.exists()
    data = blob.download_as_string().decode('utf-8')
    return data

def read_and_write_to_dataform(bucket_name, file_name, **kwargs):
    # Read data from GCS
    data = read_file_from_gcs(bucket_name, file_name)
    # Write data to Dataform
    write_to_dataform(file_name, data, **kwargs)

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

    make_directory = DataformMakeDirectoryOperator(
        task_id="make-directory",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        directory_path="definitions/OracleFinance",
    )

    list_files_task = PythonOperator(
        task_id='list_files',
        python_callable=list_files_in_bucket,
        op_kwargs={'bucket_name': BUCKET_NAME},
    )

    file_names = list_files_task.execute(context={})

    for dataset_name in file_names:
        print(dataset_name)
        # Task to read file from GCS and write to Dataform
        read_write_task = PythonOperator(
            task_id=f'read_write_to_dataform_{dataset_name}',
            python_callable=read_and_write_to_dataform,
            op_kwargs={'bucket_name': BUCKET_NAME, 'file_name': dataset_name},
            provide_context=True,
        )

        make_repository >> make_workspace >> first_initialization_step, last_initialization_step >> install_npm_packages >> make_directory >> list_files_task >> read_write_task