"""Plugin tests."""

import io
import os
from collections.abc import Generator
from dataclasses import dataclass

import pytest
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import (
    create_resource,
)
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.typed_entities.file import FileEntitySchema, ProjectFile
from cmem_plugin_base.dataintegration.typed_entities.quads import QuadEntitySchema
from cmem_plugin_base.testing import TestExecutionContext

from cmem_plugin_sparql_anything.sparql_anything_workflow import SPARQLAnything

needs_cmem = pytest.mark.skipif(
    os.environ.get("CMEM_BASE_URI", "") == "", reason="Needs CMEM configuration"
)

PROJECT_NAME = "sparql_anything_test_project"
DATASET_NAME = "sample_dataset"
RESOURCE_NAME = "sample_dataset.txt"
DATASET_TYPE = "text"


@dataclass
class FixtureEnvironmentData:
    """FixtureData testing plugin"""

    project: str
    dataset: str
    resource: str


@pytest.fixture
def di_environment() -> Generator[FixtureEnvironmentData]:
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


@needs_cmem
def test_success(di_environment: FixtureEnvironmentData) -> None:
    """Test SPARQLAnything success flow"""
    schema = FileEntitySchema()
    entity = schema.to_entity(ProjectFile(path=di_environment.resource))
    input_entities = Entities(entities=iter([entity]), schema=schema)

    output = SPARQLAnything().execute(
        [input_entities], TestExecutionContext(project_id=di_environment.project)
    )
    quads = list(QuadEntitySchema().from_entities(output).values)
    assert len(quads) == 2  # noqa: PLR2004


@needs_cmem
def test_no_input(di_environment: FixtureEnvironmentData) -> None:
    """Test error when no input file is given"""
    with pytest.raises(ValueError, match=r"No input file was given\."):
        SPARQLAnything().execute([], TestExecutionContext(project_id=di_environment.project))
