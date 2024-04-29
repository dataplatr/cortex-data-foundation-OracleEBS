"""
Creates stored procedure in BigQuery Ods dataset.
"""

import csv
import datetime
import json
import logging
import shutil
import sys
import yaml
from pathlib import Path

#from google.cloud import bigquery

from common.py_libs.bq_helper import table_exists, create_table
from common.py_libs.configs import load_config_file
from google.cloud import bigquery

# NOTE: All paths here are relative to the root directory, unless specified
# otherwise.

_THIS_DIR = Path(__file__).resolve().parent

# Config file containing various parameters.
_CONFIG_FILE = Path(_THIS_DIR, "../../config/config.json")

def create_sp_UpdateAirflowRunAuditInfo(client,project_id,ods_dataset,location):
    sql = f"""
    CREATE OR REPLACE PROCEDURE `{project_id}.{ods_dataset}.sp_UpdateAirflowRunAuditInfo`(RunId STRING, CompletedDate DATETIME, RunStatus STRING, FailureReason STRING)
    BEGIN
        UPDATE `{project_id}.{ods_dataset}.AirflowRunAuditLog`
        SET
            LastCompletedDate=CompletedDate,
            Status = RunStatus,
            Reason = FailureReason
        WHERE 
            RunAuditId = RunId;
    END;
    """
    query_job=client.query(sql,location=location)

def create_sp_InsertAirflowRunAuditInfo(client,project_id,ods_dataset,location):
    sql = f"""
    CREATE OR REPLACE PROCEDURE `{project_id}.{ods_dataset}.sp_InsertAirflowRunAuditInfo`(RunAuditId STRING, SourceName STRING, ParentSystemId INT64, SystemId INT64, Username STRING, JobName STRING, ScheduledStartDate DATETIME)
    BEGIN
        INSERT INTO `{project_id}.{ods_dataset}.AirflowRunAuditLog` (
            RunAuditId,
            SourceName,
            ParentSystemId,
            SystemId,
            Username,
            JobName,
            ScheduledStartDate
        )
        VALUES (
            RunAuditId,
            SourceName,
            ParentSystemId,
            SystemId,
            Username,
            JobName,
            ScheduledStartDate
        );
    END;
    """
    query_job=client.query(sql,location=location)

def main():
    logging.basicConfig(level=logging.INFO)

    # Lets load configs to get various parameters needed for the dag generation.
    config_dict = load_config_file(_CONFIG_FILE)
    logging.info(
        "\n---------------------------------------\n"
        "Using the following config:\n %s"
        "\n---------------------------------------\n",
        json.dumps(config_dict, indent=4))

    project_id = config_dict.get("projectId")
    ods_dataset = config_dict.get("ORACLE").get("datasets").get("Ods")
    location = config_dict.get("location", "US")

    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  project_id = %s \n"
        "  ods_dataset = %s \n"
        "  location = %s \n"
        "---------------------------------------\n", project_id, ods_dataset,
        location)
    
    # Process tables based on configs from settings file
    logging.info("Reading configs...")

     # Initialize BigQuery client
    client = bigquery.Client()

    # Call functions to create stored procedures
    create_sp_UpdateAirflowRunAuditInfo(client,project_id,ods_dataset,location)
    create_sp_InsertAirflowRunAuditInfo(client,project_id,ods_dataset,location)

if __name__ == "__main__":
    main()
