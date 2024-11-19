"""SPARQL Anything jar bin module"""

import os
from pathlib import Path

import requests

JAR_VERSION = "0.9.0"
JAR_FILE_NAME = f"sparql-anything-{JAR_VERSION}.jar"


def has_jar() -> bool:
    """Check the jar has been successfully downloaded in the installation folder."""
    files = os.listdir(get_module_path())
    return any(".jar" in file for file in files)


def get_jar_artifact_uri() -> str:
    """Retrieve the download url for latest SPARQL Anything release."""
    return f"https://download.eccenca.com/cmem-plugin-sparql-anything/sparql-anything-{JAR_VERSION}.jar"


def download_sparql_anything() -> None:
    """Download the passed version of the SPARQL Anything jar."""
    path2jar = Path(get_module_path()) / JAR_FILE_NAME
    dl_link = get_jar_artifact_uri()
    request = requests.get(dl_link, stream=True, timeout=10.0)
    request.raise_for_status()
    with (
        path2jar.open("wb") as jar,
    ):
        for data in request.iter_content(chunk_size=1024):
            jar.write(data)


def get_module_path() -> str:
    """Return the path to the PySPARQL Anything installation folder."""
    return os.path.realpath(Path(__file__).parent)


def get_path2jar() -> str:
    """Return the path to the PySPARQL Anything jar currently in use in the installation folder."""
    return str(Path(get_module_path()) / JAR_FILE_NAME)
