"""cmem-plugin-sparql-anything"""

import logging

import requests

from cmem_plugin_sparql_anything import utils

try:
    if not utils.has_jar():
        utils.download_sparql_anything()
except requests.exceptions.RequestException:
    logger = logging.getLogger(__name__)
    logger.info("Failed to download sparql anything zip file")
    raise
