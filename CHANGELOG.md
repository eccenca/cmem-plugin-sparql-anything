# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](https://semver.org/)

## [Unreleased]

### Changed

- replaced the "File" dropdown parameter with a file input port
- replaced the "Graph" / "Replace Graph" parameters and direct graph upload with a triple output port
- request N-Triples output from the sparql-anything jar instead of Turtle, and parse it
  accordingly, to avoid `BadSyntax` errors from prefixed names that Jena's Turtle writer and
  rdflib's Turtle parser disagree on

### Added

- initial version
- SPARQL Construct over files
- direct upload to graphs

