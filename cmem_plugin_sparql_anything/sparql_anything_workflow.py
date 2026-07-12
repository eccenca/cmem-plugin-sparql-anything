"""Random values workflow plugin module"""

import shlex
import tempfile
from collections.abc import Iterator, Sequence
from pathlib import Path
from subprocess import CompletedProcess, run
from typing import IO, cast

from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import Icon, Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import (
    Entities,
)
from cmem_plugin_base.dataintegration.parameter.code import SparqlCode
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import FixedNumberOfInputs, FixedSchemaPort
from cmem_plugin_base.dataintegration.typed_entities.file import File, FileEntitySchema
from cmem_plugin_base.dataintegration.typed_entities.quads import (
    BlankNode,
    ConcreteNode,
    DataTypeLiteral,
    LanguageLiteral,
    PlainLiteral,
    Quad,
    QuadEntitySchema,
    RdfNode,
    Resource,
)
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access
from rdflib import BNode, URIRef
from rdflib import Graph as RdfGraph
from rdflib import Literal as RdfLiteral
from rdflib.term import Node

from cmem_plugin_sparql_anything.constants import (
    DEFAULT_SPARQL,
    DOCUMENTATION,
    QUERY_PARAMETER_DESCRIPTION,
    SPARQL_ANYTHING_ERROR_PATTERN,
)
from cmem_plugin_sparql_anything.utils import get_path2jar


def _to_rdf_node(term: Node) -> RdfNode:
    """Convert an rdflib term into a Quad-compatible RDF node."""
    if isinstance(term, URIRef):
        return Resource(value=str(term))
    if isinstance(term, BNode):
        return BlankNode(value=str(term))
    if isinstance(term, RdfLiteral):
        if term.language:
            return LanguageLiteral(value=str(term), language=term.language)
        if term.datatype:
            return DataTypeLiteral(value=str(term), data_type=str(term.datatype))
        return PlainLiteral(value=str(term))
    raise ValueError(f"Unsupported RDF term: {term!r}")


@Plugin(
    label="SPARQL Anything",
    plugin_id="cmem_plugin_sparql_anything",
    description="Query anything with SPARQL to construct Knowledge Graphs.",
    documentation=DOCUMENTATION,
    icon=Icon(file_name="logo.svg", package=__package__),
    parameters=[
        PluginParameter(name="query", label="Query", description=QUERY_PARAMETER_DESCRIPTION),
    ],
)
class SPARQLAnything(WorkflowPlugin):
    """SPARQL Anything Workflow Plugin: Query file to generate knowledge graph"""

    def __init__(
        self,
        query: SparqlCode = DEFAULT_SPARQL,
    ) -> None:
        self.query = str(query)
        self.input_ports = FixedNumberOfInputs([FixedSchemaPort(schema=FileEntitySchema())])
        self.output_port = FixedSchemaPort(schema=QuadEntitySchema())

    def execute(
        self,
        inputs: Sequence[Entities],
        context: ExecutionContext,
    ) -> Entities:
        """Run the workflow operator."""
        self.log.info("Start querying resource")
        setup_cmempy_user_access(context.user)

        if len(inputs) == 0:
            raise ValueError("No input file was given.")

        entities = list(inputs[0].entities)
        if len(entities) != 1:
            raise ValueError(
                f"SPARQL Anything requires exactly one input file, but {len(entities)} were given."
            )

        file = FileEntitySchema().from_entity(entities[0])

        with tempfile.NamedTemporaryFile(delete=True, suffix=Path(file.path).name) as resource_file:
            self._download_resource(context.task.project_id(), file, resource_file)
            quads = list(self._parse_triples(self._run_query(resource_file.name)))
            context.report.update(
                ExecutionReport(entity_count=len(quads), operation_desc="triples generated")
            )
            return QuadEntitySchema().to_entities(iter(quads))

    def _download_resource(self, project_id: str, file: File, target: IO[bytes]) -> None:
        """Download the resource and writes it to the temporary file."""
        self.log.info("Downloading resource")
        with file.read_stream(project_id) as stream:
            for chunk in iter(lambda: stream.read(8192), b""):
                target.write(chunk)
        target.flush()

    @staticmethod
    def _parse_triples(data: bytes) -> Iterator[Quad]:
        """Parse the N-Triples output of SPARQL Anything into RDF quads."""
        rdf_graph = RdfGraph()
        rdf_graph.parse(data=data, format="nt")
        for subject, predicate, rdf_object in rdf_graph:
            yield Quad(
                subject=cast("ConcreteNode", _to_rdf_node(subject)),
                predicate=Resource(value=str(predicate)),
                object=_to_rdf_node(rdf_object),
            )

    def _run_query(self, resource: str) -> bytes:
        """Run the SPARQL Anything jar with the provided query and resource."""
        self.log.info("Start SPARQL Anything")
        with tempfile.NamedTemporaryFile(suffix=".sparql", delete=True) as query_file:
            # Replace resource placeholder in query with actual file path
            query_file.write(
                self.query.replace("{{resource_file}}", f"file://{resource}").encode("utf-8")
            )
            query_file.flush()

            # Request N-Triples output (a stricter, unambiguous serialization) instead of the
            # default Turtle, since Jena's Turtle writer can produce prefixed names that
            # rdflib's Turtle parser rejects (e.g. facade-x predicates derived from arbitrary
            # source field names).
            cmd = f"java -jar {get_path2jar()} -q {query_file.name} -f NT"
            output: CompletedProcess = run(shlex.split(cmd), check=False, capture_output=True)  # noqa: S603

        stderr = output.stderr.decode("utf-8")
        if output.returncode != 0 or SPARQL_ANYTHING_ERROR_PATTERN in stderr:
            error = stderr.partition(SPARQL_ANYTHING_ERROR_PATTERN)[2] or stderr
            raise ValueError(error.strip() or output.stdout.decode("utf-8", errors="replace"))

        return output.stdout  # type: ignore[no-any-return]
