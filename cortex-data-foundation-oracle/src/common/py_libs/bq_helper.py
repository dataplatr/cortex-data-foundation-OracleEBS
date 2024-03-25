
"""Library for BigQuery related functions."""

from collections import abc
from enum import Enum
import logging
import typing
import pathlib

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest
from google.api_core import retry
from google.cloud.exceptions import Conflict

logger = logging.getLogger(__name__)

def table_exists(bq_client: bigquery.Client, full_table_name: str) -> bool:
    """Checks if a given table exists in BigQuery."""
    try:
        bq_client.get_table(full_table_name)
        return True
    except NotFound:
        return False


def dataset_exists(bq_client: bigquery.Client, full_dataset_name: str) -> bool:
    """Checks if a given dataset exists in BigQuery."""
    try:
        bq_client.get_dataset(full_dataset_name)
        return True
    except NotFound:
        return False


DatasetExistence = Enum("DatasetExistence",
                        ["NOT_EXISTS",
                         "EXISTS_IN_LOCATION",
                         "EXISTS_IN_ANOTHER_LOCATION"])

def dataset_exists_in_location(bq_client: bigquery.Client,
                               full_dataset_name: str,
                               location: str) -> DatasetExistence:
    """Checks if a given dataset exists in BigQuery in a location."""
    try:
        dataset = bq_client.get_dataset(full_dataset_name)
        return (DatasetExistence.EXISTS_IN_LOCATION
                  if dataset.location.lower() == location.lower() # type: ignore
                  else DatasetExistence.EXISTS_IN_ANOTHER_LOCATION)
    except NotFound:
        return DatasetExistence.NOT_EXISTS

def create_table(bq_client: bigquery.Client,
                 full_table_name: str,
                 schema_tuples_list: list[tuple[str, str]],
                 exists_ok=False) -> None:
    """Creates a BigQuery table based on given schema."""
    project, dataset_id, table_id = full_table_name.split(".")

    table_ref = bigquery.TableReference(
        bigquery.DatasetReference(project, dataset_id), table_id)

    table = bigquery.Table(
        table_ref,
        schema=[bigquery.SchemaField(t[0], t[1]) for t in schema_tuples_list])

    bq_client.create_table(table, exists_ok=exists_ok)


def delete_table(bq_client: bigquery.Client, full_table_name: str) -> None:
    """ Calls the BQ API to delete the table, returns nothing """
    logger.info("Deleting table `%s`.", full_table_name)
    bq_client.delete_table(full_table_name, not_found_ok=True)
