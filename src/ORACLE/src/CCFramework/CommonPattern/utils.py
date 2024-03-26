import sys, os, json, time, math
from datetime import datetime, timezone, timedelta
import logging, typing
from google.cloud.exceptions import NotFound, GoogleCloudError
from airflow.utils.log.logging_mixin import LoggingMixin
from google.cloud import bigquery

class Utils:
    def __init__(self, name):
        self.name = name
        
    # Purpose of this module is to manage the config files that has values for common and dag variables 
    # Function read_json takes in the config file path and returns configs from json files in map variable format based on the  environment.
    # Function print config prints both the map variables that are being passed from python operator

    def read_json(self,json_file_path):
        map_variables={}
        LoggingMixin().log.info("json_file_path: {}".format(json_file_path))
#       """Reads a JSON file and returns a map variable."""
        with open(json_file_path, "r") as f:
            json_data = f.read()
        
        json_dict = json.loads(json_data)
        configs = json_dict["Configurations"]

        if configs:
             map_variables = {key: value for key, value in configs.items()}
        return map_variables
    
    # to check the arguments and pass to the dictionary 
    # declare the argumnet as None if it is empty 
    def printconfigs(self,arg1, arg2=None):
        """Prints the configuration variables in the given arguments."""
        if arg1 is not None:
            for key, value in arg1.items():
                if "SECRET" not in key.upper():
                    LoggingMixin().log.info("{}: {}".format(key, value))
        else:
            LoggingMixin().log.info("arg1 is None")
          
        if arg2 is not None:
            for key, value in arg2.items():
              if "SECRET" not in key.upper():
                LoggingMixin().log.info("{}: {}".format(key, value))
        else:
            LoggingMixin().log.info("arg2 is None")
                
        return "Printed config variable"
        
    def get_SourceExtractSql(self, ProjectId,SourceDatasetId, TableName, IsIncremental, CDCColumnName, CdcUpdateDate,SourceSystem):
        
        if SourceSystem == 'EBS':
            if IsIncremental =='Y' and CDCColumnName is not None:
                query = f"""
                    SELECT *
                    FROM `{ProjectId}.{SourceDatasetId}.{TableName}`
                    WHERE {CDCColumnName} > datetime('{CdcUpdateDate}');
                    """
            elif IsIncremental=='Y' and CDCColumnName is None:
                query = f"""
                    SELECT *
                    FROM `{ProjectId}.{SourceDatasetId}.{TableName}`;  
                    """
            elif IsIncremental=='N' and CDCColumnName is None:
                query = f"""
                        SELECT *
                        FROM `{ProjectId}.{SourceDatasetId}.{TableName}`;
                        """
            elif IsIncremental=='N' and CDCColumnName is not None:
                query = f"""
                    SELECT *
                    FROM `{ProjectId}.{SourceDatasetId}.{TableName}`
                    WHERE {CDCColumnName} > datetime('{CdcUpdateDate}');
                """
        return query
    
    def get_SourceTablePrimaryKeysSql(self, ProjectId,SourceDatasetId,TargetDatasetId,TableName,PrimaryKeys):
        query = f"""
                CREATE OR REPLACE TABLE `{ProjectId}.{TargetDatasetId}.GLOBAL_TEMP_{TableName}` AS 
                (SELECT distinct {PrimaryKeys} FROM `{ProjectId}.{SourceDatasetId}.{TableName}`);
            """
        return query
    
    def get_DynamicLoadSql(self, project_id,source_dataset, table_name, target_dataset, primary_key,source_system_id, IsIncremental, soft_delete_column, connection_id):
            # Initialize BigQuery client
            client = bigquery.Client(project=project_id )
            
            # Query column names from INFORMATION_SCHEMA.COLUMNS
            query = f"""
            SELECT column_name, data_type
            FROM `{project_id}.{source_dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
            """
            columns = [(row.column_name, row.data_type) for row in client.query(query)]

            pk_string =  ', '.join([f"'{item}'" for item in primary_key]).replace(" ", "")

            primary_key_datatype = f"""
            SELECT column_name, data_type
            FROM `{project_id}.{source_dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}' and column_name in ({pk_string})
            """ 
            primary_key_cols = [(row1.column_name, row1.data_type) for row1 in client.query(primary_key_datatype)]

            # Truncate the target table before loading the data from stage, if it is full load
            if IsIncremental == 'N':
                merge_statement = f"""
                CREATE OR REPLACE TABLE `{project_id}.{target_dataset}.TEMP_{table_name}` as 
                SELECT * FROM `{project_id}.{target_dataset}.{table_name}` where 1=2;
                MERGE `{project_id}.{target_dataset}.TEMP_{table_name}` AS target
                        USING `{project_id}.{source_dataset}.{table_name}` AS source
                        ON 1=2
                        WHEN NOT MATCHED THEN
                            INSERT ({', '.join([column_name for column_name,data_type in columns])}, UpdateBy, UpdateDate, UpdateProcess, CreateBy, CreateDate, CreateProcess,LoadBy, LoadDate, LoadProcess, ParentSystemId, SystemId, InactiveInd, InactiveReason)
                            VALUES ({', '.join([f'DATETIME(source.{column_name})' if data_type == 'DATETIME' or data_type == 'TIMESTAMP' else f'source.{column_name}' for column_name, data_type in columns])}, 'insert_airflow', current_timestamp,'insert_airflow', 'insert_airflow', current_timestamp, 'insert_airflow', 'insert_airflow', current_timestamp, 'insert_airflow', {source_system_id}, {source_system_id}, {'case when source.' + soft_delete_column + "='Y' then FALSE else TRUE end" if soft_delete_column is not None else 'FALSE'},' ');
                CREATE OR REPLACE TABLE  `{project_id}.{target_dataset}.{table_name}` as 
                SELECT * FROM `{project_id}.{target_dataset}.TEMP_{table_name}`;
                DROP TABLE `{project_id}.{target_dataset}.TEMP_{table_name}`;
            """
            elif IsIncremental == 'Y':
                merge_statement = f"""
                MERGE `{project_id}.{target_dataset}.{table_name}` AS target
                USING `{project_id}.{source_dataset}.{table_name}` AS source
                ON {' AND '.join([f'target.{pk} = {"DATETIME(source." + pk + ")" if any(data_type == "DATETIME" or data_type == "TIMESTAMP" for column_name, data_type in primary_key_cols if column_name == pk) else "source." + pk}' for pk in primary_key])}
                AND target.SystemId = {source_system_id}
                WHEN MATCHED THEN
                    UPDATE SET {', '.join([f"target.{column_name} = {'DATETIME(source.' + column_name + ')' if data_type == 'DATETIME' or data_type == 'TIMESTAMP' else 'source.' + column_name}" for column_name, data_type in columns if column_name not in primary_key])}
                    {f", target.InactiveInd = IF(source.{soft_delete_column} = 'N', TRUE, FALSE)" if soft_delete_column is not None else ''}
                    {f", target.InactiveDate = IF(source.{soft_delete_column} = 'N', current_timestamp, NULL)" if soft_delete_column is not None else ''}
                    ,target.UpdateBy = 'airflow', target.UpdateDate = current_timestamp
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join([column_name for column_name,data_type in columns])}, UpdateBy, UpdateDate, UpdateProcess, CreateBy, CreateDate, CreateProcess,LoadBy, LoadDate, LoadProcess, ParentSystemId, SystemId, InactiveInd, InactiveReason)
                    VALUES ({', '.join([f'DATETIME(source.{column_name})' if data_type == 'DATETIME' or data_type == 'TIMESTAMP' else f'source.{column_name}' for column_name, data_type in columns])}, 'insert_airflow', current_timestamp,'insert_airflow', 'insert_airflow', current_timestamp, 'insert_airflow', 'insert_airflow', current_timestamp, 'insert_airflow', {source_system_id}, {source_system_id},FALSE,' ')
            """
            
            return merge_statement
        
    def printInfo(self, arg1):
        if arg1 is not None:
            LoggingMixin().log.info(arg1)
        else:
            LoggingMixin().log.info("arg1 is none")
        return "Printed Info"
    
    #This procedure is used to get the CdcLoadDate value for the passed-in SourceSystemId, ProjectId, DataSetId and TableName from the Ods.CdcLoadDetails table
    def get_CdcLoadDate(self,source_system_id, project_id, dataset_id, table_name, connection_id):  #connection_id to be revisited for passing the credentials
        """   Retrieves the CdcLoadDate value for the specified parameters from the Ods.CdcLoadDetails table.
        Args:
            source_system_id (str): The source system ID.
            project_id (str): The GCP project ID.
            dataset_id (str): The BigQuery dataset ID.
            table_name (str): The BigQuery table name.

        Returns:
            The CdcLoadDate value as a timestamp, or None if no matching record is found.

        Raises:
            ValueError: If any input parameter is None.
        """
        if not all([source_system_id, project_id, dataset_id, table_name]):
            raise ValueError("Missing required input parameter(s).")
        client = bigquery.Client(project=project_id)
        query = f"""
                SELECT CdcLoadDate
                FROM `{project_id}.{dataset_id}.CdcLoadDetails`
                WHERE SourceSystemId = '{source_system_id}'
                AND ProjectId = '{project_id}'
                AND DataSetId = '{dataset_id}'
                AND TableName = '{table_name}'
                """

        query_job = client.query(query)

        try:
            results = query_job.result()  # Iterate over results
            row = next(results)
            return row.CdcLoadDate        
        except StopIteration:
            return None  # No matching record found
    
    
    #  This procedure is used to get the max value of the CdcUpdate column(Last_update_date) from the passed-in Ods table.
    def get_max_last_update_date(self,project_id, dataset_id, table_name, cdc_update_date_column, connection_id):
        """
        Retrieves the maximum value of the "cdc_update_date" column from the specified BigQuery table.

        Args:
            source_system_id (str): The source system ID.
            project_id (str): The GCP project ID.
            dataset_id (str): The BigQuery dataset ID.
            table_name (str): The BigQuery table name.
            cdc_update_date_column: The source update date column.

        Returns:
            The maximum value of the <cdc_update_date_column> as a timestamp, or None if the table is empty.

        Raises:
            ValueError: If any input parameter is None.
        """

        if not project_id or not dataset_id or not table_name :
            raise ValueError("Missing required input parameter(s).")

        client = bigquery.Client(project=project_id)
        query = f"""
            SELECT MAX(CAST(`{cdc_update_date_column}` AS DATETIME)) AS max_update_date
            FROM `{project_id}.{dataset_id}.{table_name}` 
        """

        query_job = client.query(query)

        try:
            results = query_job.result()  # Iterate over results
            row = next(results)
            return row.max_update_date
        except StopIteration:
            return None  # Handle empty table gracefully
        
    # This procedure is used to update the Ods.CdcLoadDetails table with the <LoadExecutionStatus> for a given <ProjectId>.<DataSetId>.<TableName>
    def update_cdc_load_status(self,project_id, dataset_id, table_name, load_execution_status):
        """
        Updates the LoadExecutionStatus in the Ods.CdcLoadDetails table for the specified parameters.

        Args:
            project_id (str): The GCP project ID.
            dataset_id (str): The BigQuery dataset ID.
            table_name (str): The BigQuery table name.
            load_execution_status (str): The new load execution status.

        Raises:
            ValueError: If any required parameter is missing.
        """

        if not all([project_id, dataset_id, table_name, load_execution_status]):
            raise ValueError("Missing required input parameter(s).")

        client = bigquery.Client(project=project_id)

        query = f"""
            UPDATE `{project_id}.Ods.CdcLoadDetails`
            SET LoadExecutionStatus = '{load_execution_status}'
            WHERE ProjectId = '{project_id}'
            AND DataSetId = '{dataset_id}'
            AND TableName = '{table_name}'
        """

        query_job = client.query(query)
        query_job.result()  # Process results

        # Optionally print success or failure message
        num_updated_rows = query_job.affected_rows
        if num_updated_rows > 0:
            print(f"Updated {num_updated_rows} records in Ods.CdcLoadDetails successfully.")
        else:
            print("No records found for the specified parameters.")


    #This procedure is used to insert/update the Ods.CdcLoadDetails table with the SourceSystemId, ProjectId, DataSetId, TableName and CdcLoadDate along with the audit fields.
    def get_UpsertCdcLoadDetails(self,source_system_id, project_id, dataset_id, table_name, cdc_load_date,load_execution_status):
        """
        Inserts or updates a record in the Ods.CdcLoadDetails table with the specified parameters.

        Args:
            source_system_id (str): The source system ID.
            project_id (str): The GCP project ID.
            dataset_id (str): The BigQuery dataset ID.
            table_name (str): The BigQuery table name.
            cdc_load_date (TIMESTAMP): The CDC load date.
            load_execution_status (str): The load execution status.
        """
        now= datetime.now()
        # DML statement for upsert
        query = f"""
            MERGE `{project_id}.Ods.CdcLoadDetails` AS T
            USING (SELECT '{source_system_id}' SourceSystemId 
                    , '{project_id}' ProjectId
                    , '{dataset_id}' DataSetId
                    , '{table_name}' TableName
                    , DATETIME('{cdc_load_date}') CdcLoadDate
                    , '{load_execution_status}' LoadExecutionStatus ) as r
            ON (T.SourceSystemId = r.SourceSystemId
            AND T.ProjectId = r.ProjectId
            AND T.DataSetId = r.DataSetId
            AND T.TableName = r.TableName)
            WHEN MATCHED THEN
                UPDATE SET
                CdcLoadDate = r.CdcLoadDate,
                LoadExecutionStatus = r.LoadExecutionStatus,
                DwUpdateDate = DATETIME('{now}')
            WHEN NOT MATCHED THEN
            INSERT (SourceSystemId, ProjectId, DataSetId, TableName, CdcLoadDate, LoadExecutionStatus,DwCreateDate,DwUpdateDate)
            VALUES (r.SourceSystemId, r.ProjectId, r.DataSetId, r.TableName, r.CdcLoadDate, r.LoadExecutionStatus,DATETIME('{now}'),DATETIME('{now}'))
            """
        return query
    
    def get_UpdateInactiveIndSql(self,project_id,connection_id, source_dataset, target_dataset,  table_name, primary_key, archive_date, archive_date_column):
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)

        ArchiveWhere=''

        if archive_date is not None and archive_date_column is not None:
            ArchiveWhere = f"""
             target.{archive_date_column} >= TIMESTAMP('{archive_date}')
            """
        elif archive_date is None and archive_date_column is not None : 
            raise ValueError("Missing required input parameter(s).")
        elif archive_date is not None and archive_date_column is None :
            raise ValueError("Missing required input parameter(s).")
            
        # Query column names from INFORMATION_SCHEMA.COLUMNS
        query = f"""
            SELECT column_name, data_type
            FROM `{project_id}.{target_dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
            """
        pk_string =  ', '.join([f"'{item}'" for item in primary_key]).replace(" ", "")
        
        primary_key_datatype = f"""
            SELECT column_name, data_type
            FROM `{project_id}.{target_dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}' and column_name in ({pk_string})
            """ 
        primary_key_cols = [(row1.column_name, row1.data_type) for row1 in client.query(primary_key_datatype)]
        
        query = f"""
            UPDATE `{project_id}.{target_dataset}.{table_name}` AS target
            SET
            InactiveInd = TRUE,
            InactiveDate = current_timestamp,
            InactiveReason = 'Hard Delete',
            UpdateDate = current_timestamp
            WHERE NOT EXISTS 
            (
            SELECT 1
            FROM `{project_id}.{source_dataset}.GLOBAL_TEMP_{table_name}` AS source
            WHERE 
            {' AND '.join([f'target.{pk} = {"DATETIME(source." + pk + ")" if any(data_type == "DATETIME" or data_type =="TIMESTAMP" for column_name, data_type in primary_key_cols if column_name == pk) else "source." + pk}' for pk in primary_key])})
            """
        if ArchiveWhere:  # Check if ArchiveWhere has a non-None value
            query += f" AND {ArchiveWhere}"  # Add ArchiveWhere as an outer filter condition

        query += f"; DROP TABLE `{project_id}.{source_dataset}.GLOBAL_TEMP_{table_name}`;"
        
        return query
    

    def export_bigquery_to_gcs(self, project_id, dataset_id, TableName, GcsPath):
        logging.basicConfig(level=logging.INFO)
        try: 
            client = bigquery.Client(project=project_id)
            dataset_ref = client.dataset(dataset_id)
            table_ref = dataset_ref.table(TableName)
            destination_uri = f"{GcsPath}/{TableName}_*.csv"
            logging.info(f"Exporting batch to GCS: {destination_uri}")

            job_config = bigquery.job.ExtractJobConfig(
                destination_format=bigquery.DestinationFormat.CSV,
                compression=None,  # No compression
            )

            extract_job = client.extract_table(
                table_ref,
                destination_uri,
                job_config=job_config
            )

            extract_job.result()

        except Exception as export_error:
            logging.error(f"Error exporting batch to GCS: {export_error}")
            raise export_error
    
    # Batch processing function
    def branch_python_function(self,IsIncremental,Source_to_OdsStage,bq_to_gcs,gcs_to_bq_task):
        if IsIncremental=='Y':
            return Source_to_OdsStage
        else:
            return [bq_to_gcs, gcs_to_bq_task]
            



