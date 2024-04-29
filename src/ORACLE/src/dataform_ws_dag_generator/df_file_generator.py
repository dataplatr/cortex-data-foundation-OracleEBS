# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Generates DAG and related files needed to copy/move Salesforce data from
RAW dataset to CDC dataset.
"""

import csv
import datetime
import json
import logging
import sys
import yaml
from pathlib import Path
import typing

from google.cloud import bigquery
from common.py_libs.configs import load_config_file
from common.py_libs.dag_generator import generate_file_from_template

# NOTE: All paths here are relative to the root directory, unless specified
# otherwise.

_THIS_DIR = Path(__file__).resolve().parent

# Config file containing various parameters.
_CONFIG_FILE = Path(_THIS_DIR, "../../config/config.json")

# Settings file containing tables to be copied from SFDC.
_SETTINGS_FILE = Path(_THIS_DIR, "../../config/table_settings.yaml")

_SQLX_FILEPATH= Path(_THIS_DIR, "../dataform_templates/")

_SOURCE_FILEPATH=Path(_THIS_DIR, "../dataform_templates/source_files")

# _TEMPLATE_FILE_PREFIX = "sfdc_raw_to_cdc_"
# _TEMPLATE_SQL_NAME = "template"

# Directory under which all the generated sql files will be created.
_GENERATED_SQLX_DIR = "generated_sql/oracle/dataform/sqlx_scripts"

# # Directory containing various template files.
# _TEMPLATE_DIR = Path(_THIS_DIR, "templates")
# # Directory containing various template files.
# _SQL_TEMPLATE_DIR = Path(_TEMPLATE_DIR, "sql")


def process_table(table_setting, edw_dataset,ods_dataset):
    """For a given table config, creates required tables as well as
    dag and related files. """

    # base_table = table_setting["base_table"]
    # source_file = table_setting["source"]
    # edw_table = table_setting["raw_table"]

    if "base_table" in table_setting:
        base_table = table_setting["base_table"]
        logging.info("__ Processing table '%s' __", base_table)
        sqlx_file = (_SQLX_FILEPATH/  f"{base_table}.sqlx").resolve()
        print(sqlx_file)
        sql_file_name = (base_table.replace(".", "_") +".sqlx")
        output_sql_file = Path(_GENERATED_SQLX_DIR, sql_file_name)
        sql_subs = {
        "edw_dataset": edw_dataset
        }
        generate_file_from_template(sqlx_file, output_sql_file, **sql_subs)
        logging.info(f"Generated SQLX file {sql_file_name}.")

    else:
        source_table = table_setting["source"]
        source_file = (_SOURCE_FILEPATH/  f"{source_table}.js").resolve()
        print(source_file)
        source_file_name = (source_table.replace(".", "_") +".js")
        output_source_file = Path(_GENERATED_SQLX_DIR, source_file_name)
        sql_subs = {
        "edw_dataset": edw_dataset,
        "ods_dataset": ods_dataset
        }
        generate_file_from_template(source_file, output_source_file,**sql_subs)
        logging.info(f"Generated source file {source_file_name}.")

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Generating dataform tables...")

    # Lets load configs to get various parameters needed for the dag generation.
    config_dict = load_config_file(_CONFIG_FILE)
    logging.info(
        "\n---------------------------------------\n"
        "Using the following config:\n %s"
        "\n---------------------------------------\n",
        json.dumps(config_dict, indent=4))

    # Read params from the config
    edw_dataset = config_dict.get("ORACLE").get("datasets").get("Edw")
    ods_dataset = config_dict.get("ORACLE").get("datasets").get("Ods")

    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  edw_dataset = %s \n"
        "  ods_dataset = %s \n"
        "---------------------------------------\n", edw_dataset,ods_dataset)

    Path(_GENERATED_SQLX_DIR).mkdir(exist_ok=True, parents=True)

    # Process tables based on table settings from settings file
    logging.info("Reading dataform table settings...")

    if not Path(_SETTINGS_FILE).is_file():
        logging.warning(
            "File '%s' does not exist. Skipping dataform file generation.",
            _SETTINGS_FILE)
        sys.exit()

    with open(_SETTINGS_FILE, encoding="utf-8") as settings_file:
        settings = yaml.load(settings_file, Loader=yaml.SafeLoader)

    if not settings:
        logging.warning("File '%s' is empty. Skipping CDC DAG generation.",
                        _SETTINGS_FILE)
        sys.exit()

    # TODO: Check settings file schema.

    if not "edw_oracle_tables" in settings:
        logging.warning(
            "File '%s' is missing property `edw_oracle_tables`. "
            "Skipping dataform file generation.", _SETTINGS_FILE)
        sys.exit()

    logging.info("Processing tables...")

    table_settings = settings["edw_oracle_tables"]+settings["df_sources"]
    for table_setting in table_settings:
        process_table(table_setting, edw_dataset,ods_dataset)

    logging.info("Done generating dataform tables .")


if __name__ == "__main__":
    main()