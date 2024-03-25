import os, sys, json, csv

#Get the dag filepath
filePath=os.path.abspath(__file__)

#Include the Utilities Folder path in the system path 
utilityDirName="CommonPattern"
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(filePath)))+os.sep+utilityDirName)


#Import the utility and config libraries

import config , utils, runaudit
from config import Config
from utils import Utils
from libraries import *
from runaudit import *


Current_date_utc = datetime.utcnow().strftime('%Y%m%d')

# Create Config and Utility Objects
configObj = Config("config_init")
utilsObj = Utils("utils_init")
runAuditObj = RunAudit("runaudit_init")

dagName = configObj.getDagName(filePath)
dagId = configObj.getDagId(dagName,filePath)

# Path to the configurations Directory
configurationsDirPath = configObj.getConfigurationsPath(filePath)

# Absolute path configurations Variable File
configVariablePath = configObj.getConfigVariablePath(configurationsDirPath)

# Read all the configuration variables 
config_variables = utilsObj.read_json(configVariablePath)


# ********* Instantiate configuration variables *********
BQ_PROJECT = config_variables['Project']
BQ_REGION = config_variables['DF_Region']
BQ_ODS_STAGE = config_variables['Bigquery_ODS_Stage']
BQ_ODS = config_variables['Bigquery_ODS']
INITIAL_EXTRACT_DATE= config_variables['Initial_Extract_Date']
SRC_CONN_ID = config_variables['SourceConnectionId_1']
TGT_CONN_ID = config_variables['TargetConnectionId']
DF_REPOSITORY_ID = config_variables['DF_Repository_Id']
DF_GITBRANCH = config_variables['DF_GitBranch']
DF_TAGS = config_variables['DF_Tags']

#GCS_STAGE_BUCKET="gcs-sandbox-ods-stage"
#GCS_PATH=f"gs://{GCS_STAGE_BUCKET}/OracleEBS/inputdata"
#GCS_EXTRACT_PATH='OracleEBS/inputdata'
#GCS_SCHEMA_BUCKET='gcs-sandbox-ods-stage-schema'
#GCS_SCHEMA_PATH='schema'
#FILE_FORMAT='csv'
#GCS_ARCHIVE_PATH="OracleEBS/backup"

GCS_STAGE_BUCKET=config_variables['GCS_STAGE_BUCKET']
GCS_PATH=config_variables['GCS_PATH']
GCS_EXTRACT_PATH=config_variables['GCS_EXTRACT_PATH']
GCS_SCHEMA_BUCKET=config_variables['GCS_SCHEMA_BUCKET']
GCS_SCHEMA_PATH=config_variables['GCS_SCHEMA_PATH']
FILE_FORMAT=config_variables['FILE_FORMAT']
GCS_ARCHIVE_PATH=config_variables['GCS_ARCHIVE_PATH']
LOAD_TIME = pendulum.now('US/Eastern').strftime("%Y_%m_%d_%H_%M_%S")


#Error Logging Function
def on_failure_callback(context):
    try:
        LoggingMixin().log.info("DAG Failed! Updating RunAudit Table...")
        ti = context['task_instance']
        failed_dag_id= ti.dag_id
        failed_tasks = [] 
        for task_instance in context['dag_run'].get_task_instances():
            if task_instance.state == State.FAILED:
                failed_tasks.append(str(task_instance.task_id)) 
        failed_tasks_list = ', '.join(failed_tasks)
        if failed_tasks:
            failure_reason = f"Dag {failed_dag_id} Failure - one or more tasks ended with errors: {failed_tasks_list}."
            reason = failure_reason[:500]
        else:
            reason = f"Dag {failed_dag_id} Generic Failure"
        runauditid = ti.xcom_pull(task_ids="GetRunaudit")
        if runauditid is not None:
            runAuditObj.update_audit_info(arg1={'RunAuditId': runauditid, 'Status':"Failed",'Reason': reason})
    except Exception as e:
        LoggingMixin().log.error(f"An error occurred in on_failure_callback: {str(e)}")

#Success Logging Function
def on_success_callback(context):
    try:
        LoggingMixin().log.info("DAG Succeeded! Updating RunAudit Table...")
        ti = context['task_instance']
        reason = f"DAG {ti.dag_id} completed successfully..."
        runauditid = ti.xcom_pull(task_ids="GetRunaudit")
        if runauditid is not None:
            runAuditObj.update_audit_info(arg1={'RunAuditId': runauditid, 'LastCompletedDate': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Status':"Success",  'Reason': reason})
    except Exception as e:
        LoggingMixin().log.error(f"An error occurred in on_success_callback: {str(e)}")



#Dag Initiation
with DAG(
    dag_id=dagId,
    template_searchpath=['/home/airflow/gcs/dags/'],
    start_date=days_ago(1),
    default_args={
        'owner': 'airflow',
    },
    schedule_interval= f"{config_variables['Schedule']}",
    concurrency = 4,
    max_active_runs=1,
    catchup=False,
    tags=[dagName],
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
) as dag:
    #Start task - Dummy task
    Start = DummyOperator(
        task_id = 'Start',
        
    )

#Print tasks leverages the print configs from Read json file module to print the map variables
Task_Print_config_variables = PythonOperator(task_id='Print_config_variables', python_callable=utilsObj.printconfigs, op_kwargs={'arg1': config_variables, 'arg2': None},)


#Variables required for Error / Success Logging
schedule_start = datetime.now()   
formatted_start = schedule_start.strftime("%Y-%m-%d %H:%M:%S")
runauditid = f"{dagName}-{formatted_start}"
iAudit_dict = { 'RunAuditId': runauditid,'SourceName':dagName,'ParentSystemId':dagName,  'SystemId': dagName, 'Username': SRC_CONN_ID,'JobName': dagName, 'ScheduledStartDate':formatted_start} 

#Task to Log Error / Success information 
Task_InsertRunAudit = PythonOperator(
        task_id='InsertRunaudit', python_callable=runAuditObj.insert_audit_log,
        op_kwargs={'arg1': iAudit_dict},
        provide_context=True,
        dag=dag,
        ) 

 # Task to get the result of execute_sp_GetRunAuditInfo
Task_GetRunAudit = PythonOperator(
        task_id="GetRunaudit", python_callable=runAuditObj.get_runid_timeframe,
        provide_context=True,
        dag=dag,
    )

# task to create dataform compilation result
create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=BQ_PROJECT,
        region=BQ_REGION,
        repository_id=DF_REPOSITORY_ID,
        compilation_result={
            "git_commitish": DF_GITBRANCH
        },
        gcp_conn_id=TGT_CONN_ID,
    )
# task to create dataform workflow invocation
create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id=BQ_PROJECT,
        region=BQ_REGION,
        repository_id=DF_REPOSITORY_ID,
         workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": { "included_tags": {DF_TAGS}, "transitive_dependencies_included": True }
        },
        gcp_conn_id=TGT_CONN_ID,
    ) 
    
#End task - Dummy task
End = DummyOperator(
      task_id = 'End',
    )



Start >> Task_Print_config_variables >> Task_InsertRunAudit >> Task_GetRunAudit >> create_compilation_result >> create_workflow_invocation >> End
    
    

    
    