"""Constants module."""

from cmem_plugin_base.dataintegration.parameter.code import SparqlCode

POLICY_TEMPLATE = """
grant {{
permission java.util.PropertyPermission "user.dir", "read";
permission java.util.PropertyPermission "http://jena.hpl.hp.com/2004/07/feature/noSecurity", "read";
permission java.util.PropertyPermission "http://jena.hpl.hp.com/2004/07/feature/noCharset", "read";
permission java.util.PropertyPermission "http://jena.hpl.hp.com/2004/08/LocationMap", "read";
permission java.util.PropertyPermission "LocationMap", "read";
permission java.util.PropertyPermission "org.apache.jena.tdb.settings", "read";
permission java.util.PropertyPermission "tdb:settings", "read";
permission java.util.PropertyPermission "sun.arch.data.model", "read";
permission java.util.PropertyPermission "*", "read,write";
permission java.lang.RuntimePermission "getenv.TURN_OFF_LR_LOOP_ENTRY_BRANCH_OPT";
permission java.io.FilePermission "location-mapping.ttl", "read";
permission java.io.FilePermission "etc/location-mapping.ttl", "read";
permission java.io.FilePermission "etc/location-mapping.rdf", "read";
permission java.io.FilePermission "location-mapping.rdf", "read";

permission java.io.FilePermission "{}", "read";
permission java.io.FilePermission "{}", "read";

}};
"""

DEFAULT_SPARQL = SparqlCode("""PREFIX fx: <http://sparql.xyz/facade-x/ns/>
CONSTRUCT { ?s ?p ?o }
WHERE
{
  SERVICE <x-sparql-anything:{{resource_file}}> {
    fx:properties fx:root "https://example.org/document" .
    fx:properties fx:blank-nodes false .
    ?s  ?p  ?o .
  }
}""")

QUERY_PARAMETER_DESCRIPTION = f"""
Query to run with sparql-anything engine.

Note: resource_file place holder will be replaced with selected resource file.

Example
```
{DEFAULT_SPARQL}
```
For more information about sparql-anything please refer: https://sparql-anything.readthedocs.io/stable/
"""

SPARQL_ANYTHING_ERROR_PATTERN = "io.github.sparqlanything.cli.SPARQLAnything"

DOCUMENTATION = """[SPARQL Anything](https://sparql-anything.cc/) is a system for Semantic Web
re-engineering that allows users to ... query anything with SPARQL.

This workflow task allows for execution of SPARQL queries against the simplistic Facade-X
meta-model of SPARQL Anything.
Facade-X provides a homogeneous view over heterogeneous data sources and support multiple formats.
It allows to extract data from project files by using SPARQL Construct queries.
In order to reference, the file, you need to use the `{{resource_file}}` placeholder.
"""
