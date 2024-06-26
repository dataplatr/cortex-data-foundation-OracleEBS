# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Data Foundation Deployment UI"""


import logging
import os
import subprocess
import typing

from prompt_toolkit import PromptSession
from prompt_toolkit.cursor_shapes import CursorShape, to_cursor_shape_config
from prompt_toolkit.shortcuts import checkboxlist_dialog, radiolist_dialog
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import HTML

import google.auth

from completers import (GCPProjectCompleter,
                        RegionsCompleter,
                        StorageBucketCompleter)
from prompt import (get_value, yes_no, print_formatted, print_formatted_json)
from datasets import (prompt_for_datasets, check_datasets_locations,
                      clear_dataset_names)
from name_checker import is_bucket_name_valid
from constants import DF_TITLE


def configure(in_cloud_shell: bool,
              default_config: typing.Dict[str, typing.Any],
              existing_config: typing.Dict[str, typing.Any],
              default_project: str) -> typing.Optional[
                                            typing.Dict[str,
                                                typing.Any]]:
    """UI driver for Data Foundation configurator.

    Args:
        in_cloud_shell (bool): True if running in Cloud Shell.
        default_config (typing.Dict[str, typing.Any]): default configuration
                                                       dictionary
        existing_config (typing.Dict[str, typing.Any]): loaded configuration
                                                        dictionary
        default_project (str): default project id to use

    Returns:
        typing.Optional[ typing.Dict[str, typing.Any]]: configured configuration
                                                        dictionary
    """

    config = default_config
    went_with_existing = False
    # If source project, target project, location and target bucket
    # are specified in config.json,
    # we assume it's initialized, and use existing config.
    if (existing_config.get("projectId", "") != "" and
         existing_config.get("location", "") != "" and
          existing_config.get("targetBucket", "") != ""):
        if yes_no(
            f"{DF_TITLE} Configuration",
            HTML("There is an existing Data Foundation configuration "
            "in <b>config/config.json:</b>\n"
            f"   Project: <b>{existing_config['projectId']}</b>\n"
            f"   Location: <b>{existing_config['location']}</b>"
            "\n\nWould you like to load it?"),
            full_screen=True
            ):
            print_formatted(
                "\n\n🦄 Using existing configuration in config.json:",
                bold=True)
            print_formatted_json(existing_config)
            config = existing_config
            went_with_existing = True

    if not default_project:
        default_project = os.environ.get("GOOGLE_CLOUD_PROJECT",
                                         None)  # type: ignore
    else:
        os.environ["GOOGLE_CLOUD_PROJECT"] = default_project

    print_formatted("\nInitializing...", italic=True, end="")
    try:
        logging.disable(logging.WARNING)
        credentials, _ = google.auth.default()
    finally:
        logging.disable(logging.NOTSET)
    project_completer = GCPProjectCompleter(credentials)
    if not default_project:
        default_project = ""

    session = PromptSession()
    session.output.erase_screen()
    print("\r", end="")
    print_formatted(f"{DF_TITLE}\n")

    defaults = ["deployORACLE"]

    while True:
        dialog = checkboxlist_dialog(
            title=HTML(DF_TITLE),
            text=HTML(
                f"Welcome to {DF_TITLE}.\n\n"
                "Please confirm the workload for deployment."),
            values=[
                ("deployORACLE", "ORACLE"),
            ],
            default_values=defaults,
            style=Style.from_dict({
                "checkbox-selected": "bg:lightgrey",
                "checkbox-checked": "bold",
            }))
        dialog.cursor = to_cursor_shape_config(CursorShape.BLINKING_UNDERLINE)
        results = dialog.run()

        if not results:
            print_formatted("See you next time! 🦄")
            return None

        config["deployORACLE"] = "deployORACLE" in results

        if (config["deployORACLE"] is False):
            if yes_no(
                    f"{DF_TITLE} Configuration",
                    "Please select one or more Data Foundation workloads.",
                    yes_text="Try again",
                    no_text="Cancel"
            ):
                continue
            else:
                print_formatted("Please check the documentation for "
                                "Cortex Data Foundation. "
                                "See you next time! 🦄")
                return None
        break

    auto_names = True
    dialog = radiolist_dialog(HTML(f"{DF_TITLE} Configuration"),
                     HTML("How would you like to configure Data Foundation "
                     "(e.g. BigQuery datasets, Cloud Storage buckets, APIs)\n"
                     "and configure permissions for deployment?\n\n"
                     "Auto-configuration will create all necessary resources "
                     "and configure permissions.\n"
                     "\n<b><u><ansibrightred>This creates a demo deployment "
                     "environments.</ansibrightred></u></b>"),
                     values=[
                ("test",
                 HTML(
                    ("<b>Use pre-configured BigQuery datasets and Storage "
                     "buckets</b> "
                    "to deploy demo environment with "
                    "auto-configuration."
                        if went_with_existing else
                    "<b>Use default BigQuery datasets and Storage bucket</b> "
                    "to deploy demo environment with "
                    "auto-configuration."))),
                ("testChooseDatasets",
                 HTML(
                    "<b>Let me choose BigQuery datasets and "
                    "Storage buckets</b> to deploy with "
                    " auto-configuration.")),
            ],
            style=Style.from_dict({
                "radio-selected": "bg:lightgrey",
                "radio-checked": "bold",
            }),
            default="test")
    dialog.cursor = to_cursor_shape_config(CursorShape.BLINKING_UNDERLINE)
    choice = dialog.run()
   
    if choice == "test":
        auto_names = True
    elif choice == "testChooseDatasets":
        auto_names = False

    project = config.get("projectId", default_project)
    if project == "":
        project = default_project
    bq_location = config.get("location", "").lower()
    config["location"] = bq_location
    dataform_ws_region=config.get("ORACLE").get("df_ws_region", "").lower()
    config["ORACLE"]["df_ws_region"]=dataform_ws_region

    if project == "":
        project = None
    if bq_location == "":
        bq_location = "us"
    if dataform_ws_region == "":
        dataform_ws_region = "us-central1"
        
    

    print_formatted("\nEnter or confirm Data Foundation configuration values:",
                    bold=True)

    project = get_value(
        session,
        "GCP Project",
        project_completer,
        project or "",
        description="Specify Data Foundation Project (existing).",
        allow_arbitrary=False,
    )
    os.environ["GOOGLE_CLOUD_PROJECT"] = project

    config["projectId"] = project

    print_formatted("Retrieving regions...", italic=True, end="")
    regions_completer = RegionsCompleter()
    print("\r", end="")


    bq_location = get_value(
            session,
            "BigQuery Location",
            regions_completer,
            default_value=bq_location.lower(),
            description="Specify GCP Location for BigQuery Datasets.",
        )
    bq_location = bq_location.lower()
    config["location"] = bq_location

    dataform_ws_region = get_value(
            session,
            "Dataform Workspace Location",
            regions_completer,
            default_value=dataform_ws_region.lower(),
            description="Specify Dataform Workspace Location .",
        )
    dataform_ws_region = dataform_ws_region.lower()
    config["ORACLE"]["df_ws_region"] = dataform_ws_region

    bucket_name = config.get("targetBucket", "")
    if bucket_name == "":
        bucket_name = f"cortex-{project}-{bq_location}"

    dag_bucket = config.get("composerDagBucket", "")

    datasets_wrong_locations = []
    if auto_names:
        datasets_wrong_locations = check_datasets_locations(config)
        if len(datasets_wrong_locations):
            auto_names = False

    if not auto_names:
        while True:
            if len(datasets_wrong_locations) > 0:
                ds_list_str = "\n".join(
                    [f"   - {ds}" for ds in datasets_wrong_locations])
                if not yes_no(DF_TITLE,
                          HTML("These datasets already exist in a location "
                               f"different than `{bq_location}`:\n"
                               f"{ds_list_str}"
                               "\n\n<b>Please choose different datasets.</b>"
                               ),
                          full_screen=True,
                          yes_text="OK",
                          no_text="CANCEL"):
                    config = None
                    break
                config = clear_dataset_names(config)
            config = prompt_for_datasets(session, config) # type: ignore
            datasets_wrong_locations = check_datasets_locations(
                                            config) # type: ignore
            if len(datasets_wrong_locations) == 0:
                break
        if not config:
            print_formatted(
                "Please check the documentation for Cortex Data Foundation. "
                "See you next time! 🦄")
            return None
        if "targetBucket" not in config or config["targetBucket"] == "":
            config["targetBucket"] = f"cortex-{project}-{bq_location}"
        bucket_completer = StorageBucketCompleter(project)
        while True:
            bucket_name = get_value(session,
                                    "Target Storage Bucket",
                                    bucket_completer,
                                    bucket_name,
                                    description="Specify Target Storage Bucket",
                                    allow_arbitrary=True)
            if not is_bucket_name_valid(bucket_name):
                if yes_no(
                    "Cortex Data Foundation Configuration",
                    f"{bucket_name} is not a valid bucket name.",
                    yes_text="Try again",
                    no_text="Cancel"
                ):
                    bucket_name = ""
                    continue
                else:
                    return None
            else:
                break
    config["targetBucket"] = bucket_name

    if "composerDagBucket" not in config or config["composerDagBucket"] == "":
        config["composerDagBucket"] = ""
    bucket_completer = StorageBucketCompleter(project)
    while True:
        dag_bucket = get_value(session,
                                "Composer Dag Bucket",
                                bucket_completer,
                                dag_bucket,
                                description="Specify Composer Dag Bucket",
                                allow_arbitrary=True)
        if not is_bucket_name_valid(dag_bucket):
            if yes_no(
                "Cortex Data Foundation Configuration",
                f"{dag_bucket} is not a valid bucket name.",
                yes_text="Try again",
                no_text="Cancel"
            ):
                dag_bucket = ""
                continue
            else:
                return None
        else:
            break
    config["composerDagBucket"] = dag_bucket    
    return config
