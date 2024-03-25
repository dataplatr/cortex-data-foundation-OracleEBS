# Import libraries - 
import sys
import os, json
import logging
import pandas  as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
from airflow.models import DAG              #library to use DAG code , main class for geenerating DAG graph and execution flow.
from airflow.utils.dates import days_ago    # ibrary used in dag schedule definiton
from airflow.models import Variable         #Library to import Airflow variables defined at Airflow UI
from airflow.operators.dummy_operator import DummyOperator    #Library to import dummy operator to create dummy tasks for execution -> Start and End Dag Tasks uses this library
from airflow.operators.bash_operator import BashOperator      #Library to import Bash Operatot to make use bash commands or to execute Shell scripts
from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator    #Library used to make Oracle Connection and extract dat to GCS as stage location 
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator      #Library used to load data from GCS to BigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator,BigQueryExecuteQueryOperator    #Latest Library used to load datga from Source Bigquery to Target Bigquery table
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import pendulum                             #Library used to set right timezone.
from google.cloud.dataform_v1beta1 import WorkflowInvocation

from airflow.utils.log.logging_mixin import LoggingMixin
#logger = logging.getLogger(__name__)
from airflow.utils.state import State
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
)