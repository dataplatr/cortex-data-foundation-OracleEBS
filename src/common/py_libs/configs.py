"""Library for Cortex Config related functions."""

import json
import logging
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)


def load_config_file(config_file) -> Dict[str, Any]:
    """Loads a json config file to a dictionary.

    Args:
        config_file: Path of config json file.

    Returns:
        Config as a dictionary.
    """
    logger.debug("Input file = %s", config_file)

    if not Path(config_file).is_file():
        raise FileNotFoundError(f"Config file '{config_file}' does not exist.")

    with open(config_file, mode="r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError:
            e_msg = f"Config file '{config_file}' is malformed or empty."
            raise Exception(e_msg) from None

        logger.info("Using the following config:\n %s",
                    json.dumps(config, indent=4))

    return config
