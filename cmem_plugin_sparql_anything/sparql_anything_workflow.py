"""Random values workflow plugin module"""

import io
import shlex
import tempfile
from collections.abc import Sequence
from subprocess import CompletedProcess, run

from cmem.cmempy.dp.proxy.graph import post_streamed
from cmem.cmempy.workspace.projects.resources.resource import get_resource_response
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import Icon, Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import (
    Entities,
)
from cmem_plugin_base.dataintegration.parameter.code import SparqlCode
from cmem_plugin_base.dataintegration.parameter.graph import GraphParameterType
from cmem_plugin_base.dataintegration.parameter.resource import ResourceParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.types import BoolParameterType
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access

from cmem_plugin_sparql_anything.constants import (
    DEFAULT_SPARQL,
    DOCUMENTATION,
    POLICY_TEMPLATE,
    QUERY_PARAMETER_DESCRIPTION,
    SPARQL_ANYTHING_ERROR_PATTERN,
)
from cmem_plugin_sparql_anything.utils import get_path2jar


@Plugin(
    label="SPARQL Anything",
    plugin_id="cmem_plugin_sparql_anything",
    description="Query anything with SPARQL to construct Knowledge Graphs.",
    documentation=DOCUMENTATION,
    icon=Icon(file_name="logo.svg", package=__package__),
    parameters=[
        PluginParameter(
            name="resource",
            label="File",
            description="Which resource file do you run query on? "
            "The dropdown shows file resources from the current project.",
            param_type=ResourceParameterType(),
        ),
        PluginParameter(name="query", label="Query", description=QUERY_PARAMETER_DESCRIPTION),
        PluginParameter(
            name="graph",
            label="Graph",
            description="Graph in which query result is stored.",
            param_type=GraphParameterType(allow_only_autocompleted_values=False),
        ),
        PluginParameter(
            name="replace_graph",
            label="Replace Graph",
            description="Enabling this option to replace graph triples.",
            param_type=BoolParameterType(),
        ),
    ],
)
class SPARQLAnything(WorkflowPlugin):
    """SPARQL Anything Workflow Plugin: Query file to generate knowledge graph"""

    def __init__(
        self,
        resource: str,
        graph: str,
        replace_graph: bool = False,
        query: SparqlCode = DEFAULT_SPARQL,
    ) -> None:
        self.resource = resource
        self.query = str(query)
        self.graph = graph
        self.replace_graph = replace_graph

    def execute(
        self,
        inputs: Sequence[Entities],  # noqa: ARG002
        context: ExecutionContext,
    ) -> None:
        """Run the workflow operator."""
        self.log.info("Start querying resource")
        setup_cmempy_user_access(context.user)

        with tempfile.NamedTemporaryFile(delete=True, suffix=self.resource) as resource_file:
            self._download_resource(context.task.project_id(), self.resource, resource_file)  # type: ignore[arg-type]
            self.post_result_to_graph(data=self._run_query(resource_file.name))
            context.report.update(ExecutionReport(entity_count=1, operation_desc="graph updated"))

    def _download_resource(self, project_id: str, resource: str, file: io.StringIO) -> None:
        """Download the resource and writes it to the temporary file."""
        self.log.info("Downloading resource")
        with get_resource_response(project_id, resource) as response:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        file.flush()

    def post_result_to_graph(self, data: bytes) -> None:
        """Post result to graph"""
        self.log.info(data.decode())
        post_streamed(graph=self.graph, file=io.BytesIO(data), replace=self.replace_graph)

    def _run_query(self, resource: str) -> bytes:
        """Run the SPARQL Anything jar with the provided query and resource."""
        self.log.info("Start SPARQL Anything")
        with (
            tempfile.NamedTemporaryFile(suffix=".sparql", delete=True) as query_file,
            tempfile.NamedTemporaryFile(suffix="anything.policy", delete=True) as policy_file,
        ):
            # Write policy content with proper resource file path
            policy_file.write(POLICY_TEMPLATE.format(query_file.name, resource).encode("utf-8"))
            policy_file.flush()

            # Replace resource placeholder in query with actual file path
            query_file.write(
                self.query.replace("{{resource_file}}", f"file://{resource}").encode("utf-8")
            )
            query_file.flush()

            # Build command with policy and query file paths
            cmd = (
                f"java -Djava.security.manager -Djava.security.policy={policy_file.name}"
                f" -jar {get_path2jar()} -q {query_file.name}"
            )
            output: CompletedProcess = run(shlex.split(cmd), check=False, capture_output=True)  # noqa: S603
        if SPARQL_ANYTHING_ERROR_PATTERN in output.stderr.decode("utf-8"):
            error = output.stderr.decode("utf-8").partition(SPARQL_ANYTHING_ERROR_PATTERN)[2]
            raise ValueError(f"{error}")

        return output.stdout  # type: ignore[no-any-return]
