"""
Processes and validates SAP Reporting config.json.
"""

import logging
from typing import Union

from common.py_libs import resource_validation_helper


def validate(cfg: dict) -> Union[dict, None]:
    """Validates and processes configuration.

    Args:
        cfg (dict): Config dictionary.

    Returns:
        dict: Processed config dictionary.
    """
    logging.info("Validating ORACLE configuration.")

    oracle = cfg.get("ORACLE", None)
    if not oracle:
        logging.error("🛑 Missing 'ORACLE' values in the config file. 🛑")
        return None

    datasets = oracle.get("datasets")
    if not datasets:
        logging.error("🛑 Missing 'ORACLE/datasets' values in the config file. 🛑")
        return None

    cfg["ORACLE"]["datasets"]["Ods"] = datasets.get("Ods", "")
    if not cfg["ORACLE"]["datasets"]["Ods"]:
        logging.error("🛑 Missing 'ORACLE/datasets/Ods' values "
                      "in the config file. 🛑")
        return None
    cfg["ORACLE"]["datasets"]["OdsStage"] = datasets.get("OdsStage", "")
    if not cfg["ORACLE"]["datasets"]["OdsStage"]:
        logging.error("🛑 Missing 'ORACLE/datasets/OdsStage' values "
                      "in the config file. 🛑")
        return None
    cfg["ORACLE"]["datasets"]["Edw"] = datasets.get("Edw",
                                                        "")
    if not cfg["ORACLE"]["datasets"]["Edw"]:
        logging.error("🛑 Missing 'ORACLE/datasets/Edw' values "
                      "in the config file. 🛑")

    datasets = oracle.get("datasets")
    projectId = cfg["projectId"]
    location = cfg["location"]
    datasets = [
        resource_validation_helper.DatasetConstraints(
            f'{projectId}.{datasets["OdsStage"]}',
            True, True, location),
        resource_validation_helper.DatasetConstraints(
            f'{projectId}.{datasets["Ods"]}',
            True, True, location),
        resource_validation_helper.DatasetConstraints(
            f'{projectId}.{datasets["Edw"]}',
            False, True, location)
        ]
    if not resource_validation_helper.validate_resources([],
                                                            datasets):
        return None

    logging.info("✅ ORACLE configuration is good.")

    return cfg
