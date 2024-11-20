"""cmem-plugin-sparql-anything"""

import logging

import requests

from cmem_plugin_sparql_anything import utils

try:
    if not utils.has_jar():
        utils.download_sparql_anything()
except requests.exceptions.RequestException:
    logging.info("Failed to download sparql anything zip file")
    raise
