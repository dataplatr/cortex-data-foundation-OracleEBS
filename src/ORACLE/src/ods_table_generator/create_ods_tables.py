"""
Generates Ods Tables to copy/move Oracle data from
Stage table to BigQuery Ods dataset.
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

# Settings file containing tables to be copied from SFDC.
_SETTINGS_FILE = Path(_THIS_DIR, "../../config/table_settings.yaml")
_SCHEMA_FILEPATH= Path(_THIS_DIR, "../ods_schema/")

def process_table(bq_client, table_config, ods_dataset, ods_project):
    base_table = table_config["base_table"]
    ods_table = ods_project + "." + ods_dataset + "." + base_table

    if not table_exists(bq_client, ods_table):
        logging.info(
            "Ods table {} doesn't exist. Creating one according to the schema mapping.".format(ods_table)
        )
        #schema_file = (_THIS_DIR.parent / "ods_edw_schema" / f"{base_table}.csv").resolve()
        schema_file = (_SCHEMA_FILEPATH/  f"{base_table}.csv").resolve()
        schema_list = []
        with open(schema_file, encoding="utf-8", newline="") as csv_file:
            for row in csv.DictReader(csv_file, delimiter=","):
                target_name = row["TargetField"]
                schema_list.append((target_name, row["DataType"]))

        create_table(bq_client, ods_table, schema_list)
    else:
        logging.info(
            "Ods table {} already exists. Skipping creation.".format(ods_table)
        )

def main():
    logging.basicConfig(level=logging.INFO)

    # Lets load configs to get various parameters needed for the dag generation.
    config_dict = load_config_file(_CONFIG_FILE)
    logging.info(
        "\n---------------------------------------\n"
        "Using the following config:\n %s"
        "\n---------------------------------------\n",
        json.dumps(config_dict, indent=4))

    ods_project = config_dict.get("projectId")
    ods_dataset = config_dict.get("ORACLE").get("datasets").get("Ods")
    location = config_dict.get("location", "US")

    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  ods_project = %s \n"
        "  ods_dataset = %s \n"
        "  location = %s \n"
        "---------------------------------------\n", ods_project, ods_dataset,
        location)
    
    # Process tables based on configs from settings file
    logging.info("Reading configs...")

    if not Path(_SETTINGS_FILE).is_file():
        logging.warning(
            "File '%s' does not exist. Skipping BigQuery Ods Table generation.",
            _SETTINGS_FILE)
        sys.exit()

    with open(_SETTINGS_FILE, encoding="utf-8") as settings_file:
        configs = yaml.load(settings_file, Loader=yaml.SafeLoader)

    if not configs:
        logging.warning("File '%s' is empty. Skipping BQ Table generation.",
                        _SETTINGS_FILE)
        sys.exit()

    if not "oracle_ods_tables" in configs:
        logging.warning(
            "File '%s' is missing property `oracle_ods_tables`. "
            "Skipping BQ Table generation.", _SETTINGS_FILE)
        sys.exit()

    logging.info("Processing tables...")

    bq_client = bigquery.Client()

    table_configs = configs["oracle_ods_tables"]
    for table_config in table_configs:
        process_table(bq_client, table_config, ods_dataset, ods_project)

    logging.info("Done creating ods tables.")


if __name__ == "__main__":
    main()
