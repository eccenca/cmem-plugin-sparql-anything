"""Plugin tests."""

import io
from collections.abc import Generator
from dataclasses import dataclass

import pytest
from cmem.cmempy.queries import SparqlQuery
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import (
    create_resource,
)

from cmem_plugin_sparql_anything.sparql_anything_workflow import SPARQLAnything
from tests.utils import TestExecutionContext, needs_cmem

PROJECT_NAME = "sparql_anything_test_project"
DATASET_NAME = "sample_dataset"
RESOURCE_NAME = "sample_dataset.txt"
DATASET_TYPE = "text"
GRAPH = "https://example.org/graph/"
TRIPLE_COUNT_QUERY = """SELECT (COUNT(*) as ?Triples)
WHERE
  { GRAPH <{{graph}}>
      { ?s ?p ?o }
  }"""


@dataclass
class FixtureEnvironmentData:
    """FixtureData testing plugin"""

    project: str
    dataset: str
    resource: str


@pytest.fixture
def di_environment() -> Generator[FixtureEnvironmentData, None, None]:
    """Provide the DI build project incl. assets."""
    make_new_project(PROJECT_NAME)
    make_new_dataset(
        project_name=PROJECT_NAME,
        dataset_name=DATASET_NAME,
        dataset_type=DATASET_TYPE,
        parameters={"file": RESOURCE_NAME},
        autoconfigure=False,
    )
    with io.StringIO("sparql-anything plugin sample file.") as response_file:
        create_resource(
            project_name=PROJECT_NAME,
            resource_name=RESOURCE_NAME,
            file_resource=response_file,
            replace=True,
        )

    yield FixtureEnvironmentData(project=PROJECT_NAME, dataset=DATASET_NAME, resource=RESOURCE_NAME)
    delete_project(PROJECT_NAME)


def _triple_count(graph: str) -> int:
    """Count the no of triple in graph"""
    query = SparqlQuery(
        TRIPLE_COUNT_QUERY,
        label="query from string to get triples count metadata",
        origin="unknown",
        placeholder={
            "graph": graph,
        },
    )
    result = query.get_json_results(
        placeholder={
            "graph": graph,
        }
    )
    count = result["results"]["bindings"][0]["Triples"]["value"]
    return int(count)


@needs_cmem
def test_success(di_environment: FixtureEnvironmentData) -> None:
    """Test SPARQLAnything success flow"""
    SPARQLAnything(di_environment.resource, GRAPH, True).execute(
        [], TestExecutionContext(project_id=di_environment.project)
    )
    assert _triple_count(GRAPH) == 2  # noqa: PLR2004
