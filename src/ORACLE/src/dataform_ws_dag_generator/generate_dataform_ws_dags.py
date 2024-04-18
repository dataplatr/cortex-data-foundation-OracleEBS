import json
import logging
import sys
from pathlib import Path

import yaml

from common.py_libs.configs import load_config_file
from common.py_libs.dag_generator import generate_file_from_template

_THIS_DIR = Path(__file__).resolve().parent

_CONFIG_FILE = Path(_THIS_DIR, "../../config/config.json")
_GENERATED_DAG_DIR = "generated_dag/create_oracle_dataform_ws_dag"
_TEMPLATE_DIR = Path(_THIS_DIR, "templates")

generated_file_prefix = "dataform_ws_dag"

# Python file generation
#########################
python_template_file = Path(_TEMPLATE_DIR, "dataform_dag.py")
output_py_file_name = generated_file_prefix + ".py"
output_py_file = Path(_GENERATED_DAG_DIR, output_py_file_name)

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Generating DAGS...")

    # Load configs to get various parameters needed for the DAG generation.
    config_dict = load_config_file(_CONFIG_FILE)
    logging.info(
        "\n---------------------------------------\n"
        "Using the following config:\n %s"
        "\n---------------------------------------\n",
        json.dumps(config_dict, indent=4))

    # Read params from the config
    project_id = config_dict.get("projectId")
    bucket_name=config_dict.get("targetBucket")
    dataform_ws_region = config_dict.get("ORACLE").get("df_ws_region")
    repository_id = config_dict.get("ORACLE").get("df_repository_id")
    workspace_id = config_dict.get("ORACLE").get("df_workspace_id")

    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  project = %s \n"
        "  dataform_ws_region = %s \n"
        "  bucket_name = %s \n"
        "  repository_id = %s \n"
        "  workspace_id = %s \n"
        "---------------------------------------\n", project_id, dataform_ws_region,bucket_name, repository_id,workspace_id)

    Path(_GENERATED_DAG_DIR).mkdir(exist_ok=True, parents=True)



    # Pass parameters to the py_subs dictionary
    py_subs = {
        "project_id": project_id,
        "dataform_ws_region": dataform_ws_region,
        "repository_id" : repository_id,
        "workspace_id" : workspace_id,
        "bucket_name" : bucket_name
    }

    # Generate Python file from template
    generate_file_from_template(python_template_file, output_py_file, **py_subs)

    logging.info("Generated Dataform DAG Python files ")

if __name__ == "__main__":
    main()
