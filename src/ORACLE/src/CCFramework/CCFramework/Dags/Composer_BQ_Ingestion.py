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
ORACLE_EBS_CONN_ID = config_variables['Oracle_Conn_Id']
GCP_CONN_ID=SRC_CONN_ID

GCS_STAGE_BUCKET=config_variables['GCS_STAGE_BUCKET']
GCS_PATH=config_variables['GCS_PATH']
GCS_EXTRACT_PATH=config_variables['GCS_EXTRACT_PATH']
GCS_SCHEMA_BUCKET=config_variables['GCS_SCHEMA_BUCKET']
GCS_SCHEMA_PATH=config_variables['GCS_SCHEMA_PATH']
FILE_FORMAT=config_variables['FILE_FORMAT']
GCS_ARCHIVE_PATH=config_variables['GCS_ARCHIVE_PATH']
LOAD_TIME = pendulum.now('US/Eastern').strftime("%Y_%m_%d_%H_%M_%S")


# Get the source table list which is the .csv file location
SourceTableList = configObj.getSourceTableList(configurationsDirPath,config_variables['SourceTableListFileName'])

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

#archive task - Bash command to move data from extracted path to archive path once data loads completed.
Task_Archive = BashOperator(
        task_id="archive_task",
        #trigger_rule='none_failed_or_skipped',
        bash_command="""
            if gsutil -q stat gs://{}/{}/*.csv; then
                gsutil -m mv gs://{}/{}/*.csv gs://{}/{}/{}/
            else
                echo "No files found to move."
            fi
        """.format(GCS_STAGE_BUCKET, GCS_EXTRACT_PATH, GCS_STAGE_BUCKET,GCS_EXTRACT_PATH, GCS_STAGE_BUCKET, GCS_ARCHIVE_PATH, LOAD_TIME),
        execution_timeout=timedelta(hours=1),
    )

#Print tasks leverages the print configs from Read json file module to print the map variables
Task_Print_config_variables = PythonOperator(task_id='Print_config_variables', python_callable=utilsObj.printconfigs, op_kwargs={'arg1': config_variables, 'arg2': None},)

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(SourceTableList)
df = df.replace({np.nan: None})

#Variables required for Error / Success Logging
schedule_start = datetime.now()
formatted_start = schedule_start.strftime("%Y-%m-%d %H:%M:%S")
runauditid = f"{dagName}-{formatted_start}"
iAudit_dict = { 'RunAuditId': runauditid,'SourceName': df.iloc[0]['SourceDatasetId'],'ParentSystemId': config_variables["SourceSystemId"][df.iloc[0]['SourceSystem']],  'SystemId': config_variables["SourceSystemId"][df.iloc[0]['SourceSystem']], 'Username': SRC_CONN_ID,'JobName': dagName, 'ScheduledStartDate':formatted_start} 

# Task to Log Error / Success information 
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

Task_Trigger_DataformDag = TriggerDagRunOperator(
              task_id="TEMP_Dataform_Execution",
              trigger_dag_id="CCFrameWork_TEMP_Dataform_Execution",
              wait_for_completion=False,
              dag=dag )
    
#End task - Dummy task
End = DummyOperator(
      task_id = 'End',
    )

# Iterate through each row in the DataFrame
for row in df.itertuples():
    # Access each column value by its header and assign it to a variable
    SourceSystem = df['SourceSystem'].at[row.Index]
    SourceSystemId = config_variables["SourceSystemId"][SourceSystem]
    SourceDatasetId = df['SourceDatasetId'].at[row.Index]
    TableName = df['TableName'].at[row.Index]
    IsIncremental = df['IsIncremental'].at[row.Index]
    PrimaryKeys = df['PrimaryKey'].at[row.Index].replace(" ", "")
    PrimaryKeyList = df['PrimaryKey'].at[row.Index].split(',')
    UpdateKey = df['UpdateKey'].at[row.Index]
    CDCColumnName = df['CDCColumnName'].at[row.Index]
    SoftDeleteColumn = df['SoftDeleteColumn'].at[row.Index]
    ArchiveDate = df['ArchiveDate'].at[row.Index]
    ArchiveDateColumn = df['ArchiveKey'].at[row.Index]
    CdcUpdateDate = utilsObj.get_CdcLoadDate(SourceSystemId, BQ_PROJECT, BQ_ODS, TableName, TGT_CONN_ID)
    if CdcUpdateDate is None or IsIncremental=='N':
        CdcUpdateDate = INITIAL_EXTRACT_DATE
    ExtractSql = utilsObj.get_SourceExtractSql(BQ_PROJECT, SourceDatasetId, TableName, IsIncremental, CDCColumnName, CdcUpdateDate,SourceSystem)
    TempTableSql = utilsObj.get_SourceTablePrimaryKeysSql(BQ_PROJECT,SourceDatasetId,BQ_ODS_STAGE,TableName,PrimaryKeys)
    MergeSql = utilsObj.get_DynamicLoadSql(BQ_PROJECT,BQ_ODS_STAGE, TableName, BQ_ODS, PrimaryKeyList,SourceSystemId,IsIncremental,SoftDeleteColumn,TGT_CONN_ID)
    
    oracle_to_gcs_task = OracleToGCSOperator(
            task_id="oracle_to_gcs_{}".format(TableName),
            oracle_conn_id=ORACLE_EBS_CONN_ID,
            sql="{}".format(ExtractSql),
            bucket=GCS_STAGE_BUCKET,
            filename=f'{GCS_EXTRACT_PATH}/{TableName}.{FILE_FORMAT}',
            field_delimiter=',',
            export_format='csv',
            gzip=False, 
            gcp_conn_id=GCP_CONN_ID,
        )

    # Source_to_OdsStage = BigQueryExecuteQueryOperator(
    #         task_id="{}_Source.{}_To_OdsStage.{}".format(row.Index,TableName,TableName),
    #         sql=ExtractSql,
    #         use_legacy_sql=False,
    #         create_disposition='CREATE_NEVER',
    #         write_disposition='WRITE_TRUNCATE',
    #         destination_dataset_table='{}.{}'.format(BQ_ODS_STAGE,TableName),
    #         gcp_conn_id=SRC_CONN_ID,
    #     )

    gcs_to_bq_task = GCSToBigQueryOperator(
            task_id="{}-Task_{}_gcs_to_bq".format(row.Index,TableName),
            schema_object_bucket=GCS_SCHEMA_BUCKET,
            bucket=GCS_STAGE_BUCKET,
            source_format=FILE_FORMAT,
            source_objects=f"{GCS_EXTRACT_PATH}/{TableName}*.csv",
            destination_project_dataset_table='.'.join([BQ_PROJECT, BQ_ODS_STAGE, TableName]),
            schema_object='{}/{}.json'.format(GCS_SCHEMA_PATH,TableName),
            create_disposition='CREATE_IF_NEEDED', 
            write_disposition='WRITE_TRUNCATE',
            skip_leading_rows=1,
            allow_quoted_newlines=True,
            field_delimiter=',',
            gcp_conn_id=SRC_CONN_ID,
        )
    
    # #This task gathers all primary key data from the source into a temporary table. This is used further to check the hard deletes in the source.
    # Source_to_OdsStage_PrimaryKeyData = BigQueryExecuteQueryOperator(
    #         task_id="{}_Source.{}_To_GLOBAL_TEMP_{}".format(row.Index,TableName,TableName),
    #         #trigger_rule='none_failed_or_skipped',
    #         sql=TempTableSql,
    #         use_legacy_sql=False,
    #         gcp_conn_id=SRC_CONN_ID,
    #         dag = dag,
    #     )
    
    # Define OracleOperator to execute the SQL query on Oracle EBS source
    Source_to_OdsStage_PrimaryKeyData = OracleOperator(
        task_id="{}_Source.{}_To_GLOBAL_TEMP_{}".format(row.Index, TableName, TableName),
        sql=TempTableSql,
        oracle_conn_id=ORACLE_EBS_CONN_ID,
        dag=dag,
    )

    # This task merges the data from OdsStage to Ods table
    OdsStage_to_Ods = BigQueryInsertJobOperator(
            task_id="{}_OdsStage.{}_To_Ods.{}".format(row.Index,TableName,TableName),
            #trigger_rule='none_failed_or_skipped',
            configuration={
                "query": {
                    "query": MergeSql,
                    "useLegacySql":False,
                    "allow_large_results":True,
                }
            },
            params={'BQ_PROJECT': BQ_PROJECT }, # 'BQ_EDW_DATASET': BQ_EDW_DATASET, 'BQ_STAGING_DATASET': BQ_STAGING_DATASET },
            gcp_conn_id=TGT_CONN_ID,
            #location=BQ_REGION,
        )
    
    # This variable holds the MAX CdcUpdateDate in the Ods table
    MaxLastUpdateDate = utilsObj.get_max_last_update_date(BQ_PROJECT, BQ_ODS, TableName, CDCColumnName,TGT_CONN_ID)
    
    # If the data is not loaded in Ods table, then set the MaxCdcUpdateDate to CdcUpdateDate which would contain INITIAL_EXTRACT_DATE
    if MaxLastUpdateDate is None: MaxLastUpdateDate = CdcUpdateDate
    
    # This variable holds the CdcUpsert Query
    CdcUpsertSql = utilsObj.get_UpsertCdcLoadDetails(SourceSystemId, BQ_PROJECT, BQ_ODS, TableName, MaxLastUpdateDate,'Complete')

    # This variable holds the HardDelete Query
    InactiveIndSql = utilsObj.get_UpdateInactiveIndSql(BQ_PROJECT,TGT_CONN_ID, BQ_ODS_STAGE, BQ_ODS,  TableName, PrimaryKeyList,ArchiveDate,ArchiveDateColumn)
    
    # This task upserts the data into CdcLoadDetails table
    Upsert_Cdc = BigQueryInsertJobOperator(
    task_id="{}_CdcLoadDetails-{}".format(row.Index,TableName),
    #trigger_rule='none_failed_or_skipped',
    configuration={
                "query": {
                    "query":CdcUpsertSql,
                    "useLegacySql":False,
                    "allow_large_results":True,
                }
            },
    params={'BQ_PROJECT': BQ_PROJECT }, # 'BQ_EDW_DATASET': BQ_EDW_DATASET, 'BQ_STAGING_DATASET': BQ_STAGING_DATASET },
    gcp_conn_id=TGT_CONN_ID,
    #location=BQ_REGION,
        )
    
    # This task updates the source hard deleted records in Ods table
    Update_Inactive_Indicator = BigQueryInsertJobOperator(
    task_id="{}_UpdateInactiveInd_{}".format(row.Index,TableName),
    configuration={
                "query": {
                    "query": InactiveIndSql,
                    "useLegacySql":False,
                    "allow_large_results":True,
                }
            },
    params={'BQ_PROJECT': BQ_PROJECT }, # 'BQ_EDW_DATASET': BQ_EDW_DATASET, 'BQ_STAGING_DATASET': BQ_STAGING_DATASET },
    gcp_conn_id=TGT_CONN_ID,
    #location=BQ_REGION,
        )
    
     # Start >> Source_to_OdsStage >>Source_to_OdsStage_PrimaryKeyData >> End

    Start >> Task_Print_config_variables >> Task_InsertRunAudit >> Task_GetRunAudit >> oracle_to_gcs_task >> gcs_to_bq_task >> Source_to_OdsStage_PrimaryKeyData >> OdsStage_to_Ods >> Upsert_Cdc >> Update_Inactive_Indicator >> Task_Archive >> Task_Trigger_DataformDag >>End