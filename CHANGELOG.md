# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](https://semver.org/)

## [Unreleased]

### Fixed

- stopped passing `-Djava.security.manager -Djava.security.policy=...` to the sparql-anything
  jar: the Security Manager API was permanently removed in JDK 24+ (JEP 486), so on hosts
  running a current JDK the jar's JVM failed to start and its "Error occurred during
  initialization of VM" message was fed to the RDF parser as if it were query output, causing
  a `WorkflowExecutionException` for every execution
- download the file via `File.read_bytes()` instead of manually copying `read_stream()`
  chunks: for project resources served with `Content-Encoding: gzip`, `read_stream()` returns
  the still-compressed bytes (it reads the HTTP response's raw stream, bypassing
  Content-Encoding handling), so the jar was fed gzip binary instead of the file content,
  producing garbled triples

### Changed

- replaced the "File" dropdown parameter with a file input port
- replaced the "Graph" / "Replace Graph" parameters and direct graph upload with a triple output port
- request N-Triples output from the sparql-anything jar instead of Turtle, and parse it
  accordingly, to avoid `BadSyntax` errors from prefixed names that Jena's Turtle writer and
  rdflib's Turtle parser disagree on
- `_run_query` now also treats a non-zero jar exit code as an error, regardless of which
  stream the error text was written to

### Added

- initial version
- SPARQL Construct over files
- direct upload to graphs

