
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
"""Dataset manager for the interactive deployer"""

import typing

from google.cloud.exceptions import NotFound
from google.cloud.bigquery import Client, DatasetReference
from prompt_toolkit import PromptSession

from completers import BigQueryDatasetCompleter
from prompt import get_value, yes_no, print_formatted
from name_checker import is_dataset_name_valid
from constants import DF_TITLE

# List of dataset descriptor as tuples:
# ( list of flags or variables to be true,
#   dataset name value,
#   description,
#   target project flag ).
# Flags and dataset names are addressed as dot-separated path inside config.json
DATASETS = [
        (["deployORACLE"], "ORACLE.datasets.OdsStage", "ORACLE Raw", False),
        (["deployORACLE"], "ORACLE.datasets.Ods", "ORACLE CDC Processed",
            False),
        # (["deploySAP"], "SAP.datasets.reporting", "SAP Reporting",
        #     True),
    ]


def _get_json_value(config: typing.Dict[str, typing.Any],
                    value_path: str) -> typing.Any:
    """Get value from Data Foundation configuration dictionary

    Args:
        config (typing.Dict[str, typing.Any]): configuration dictionary
        value_path (str): path to the value in json with nodes separated by dot,
                          e.g. "SAP.datasets.raw"

    Returns:
        typing.Any: value
    """
    components = value_path.strip().split(".")
    current = config
    for component in components:
        if component not in current:
            return None
        current = current[component]
    return current


def _set_json_value(config: typing.Dict[str, typing.Any],
                    value_path: str, value: typing.Any) -> typing.Dict[
                                                            str, typing.Any]:
    """Set a value in Data Foundation configuration dictionary

    Args:
        config (typing.Dict[str, typing.Any]): configuration dictionary
        value_path (str): path to the value in json with nodes separated by dot,
                          e.g. "SAP.datasets.raw"
        value (typing.Any): value

    Returns:
        typing.Dict[str, typing.Any]: updated configuration dictionary
    """
    components = value_path.strip().split(".")
    current = config
    for component in components[:-1]:
        if component not in current:
            current[component] = {}
        current = current[component]
    current[components[-1]] = value
    return config


# def clear_dataset_names(config: typing.Dict[str, typing.Any]) -> typing.Dict[
#                                                             str, typing.Any]:
#     for dataset in DATASETS:
#         if not _is_dataset_needed(config, dataset):
#             continue
#         config = _set_json_value(config, dataset[1], "")
#     return config


def get_all_datasets(config: typing.Dict[str, typing.Any]) -> typing.List[str]:
    """Retrieves all configured datasets from Data Foundation configuration
       as a list of "{project_id}.{dataset_name}" strings.

    Args:
        config (typing.Dict[str, typing.Any]): Data Foundation configuration

    Returns:
        typing.List[str]: dataset list
    """
    datasets = []
    project = config["projectId"]
    for dataset in DATASETS:
        name = _get_json_value(config, dataset[0])
        if name and name != "":
            datasets.append((project) + "." + name)

    return datasets


# def _is_dataset_needed(config: typing.Dict[str, typing.Any],
#                        dataset: typing.Tuple[typing.List[str],
#                                              str, str, bool]) -> bool:
#     """Determines if dataset is needed by checking if the respective
#        workload needs to be deployed.

#     Args:
#         config (typing.Dict[str, typing.Any]): Data Foundation configuration
#         dataset (typing.Tuple[typing.List[str], str, str, bool]):
#                                                Dataset definition tuple

#     Returns:
#         bool: True if dataset is needed, False otherwise
#     """
#     result = True
#     for flag in dataset[0]:
#         if isinstance(flag, str):
#             value = _get_json_value(config, flag)
#             if not value:
#                 value = False
#             result = result and value
#         else:
#             result = result and bool(flag)
#     return result


def check_datasets_locations(config: typing.Dict[str, typing.Any]) -> (
                                                            typing.List[str]):
    print_formatted("Checking BigQuery datasets...", italic=True, end="")
    datasets_wrong_locations = []
    clients = {
            config["projectId"]: Client(config["projectId"],
                                           location=config["location"])
        }
    location = config["location"].lower()
    for dataset in DATASETS:
        # if not _is_dataset_needed(config, dataset):
        #     continue
        current_value = _get_json_value(config, dataset[1])
        project = (config["projectId"])

        try:
            dataset = clients[project].get_dataset(DatasetReference(project,
                                                          current_value))
            if dataset.location.lower() != location: # type: ignore
                datasets_wrong_locations.append(current_value)
        except NotFound:
            continue
    print("\r                                 \r", end="")

    return datasets_wrong_locations



def prompt_for_datasets(session: PromptSession,
                        config: typing.Dict[str, typing.Any]) -> (
                            typing.Optional[ typing.Dict[str, typing.Any]]):
    """Asks user to enter names of necessary datasets."""

    print_formatted("Accessing BigQuery...", italic=True, end="")
    project = config["projectId"]
    source_completer = BigQueryDatasetCompleter(project,
                                                Client(project=project))
    print("\r                       \r", end="")
    for dataset in DATASETS:
        # if not _is_dataset_needed(config, dataset):
        #     continue
        current_value = _get_json_value(config, dataset[1])
        if not current_value:
            current_value = ""
        while True:
            dataset_name = get_value(session, f"{dataset[2]} Dataset",
                                (source_completer),
                                description=f"{dataset[2]} Dataset",
                                default_value=current_value,
                                allow_arbitrary=True)
            if not is_dataset_name_valid(dataset_name):
                if yes_no(
                    f"{DF_TITLE} Configuration",
                    f"{dataset_name} is not a valid dataset name.",
                    yes_text="Try gain",
                    no_text="Cancel"
                ):
                    continue
                else:
                    return None
            else:
                break
        config = _set_json_value(config, dataset[1], dataset_name)

    return config
