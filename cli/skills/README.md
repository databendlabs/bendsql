# BendSQL embedded skills

This directory holds trusted, versioned text resources bundled into the BendSQL
binary in both debug and release builds. Add a skill under `<name>/SKILL.md`, with
optional Markdown reference documents alongside it. Do not add credentials,
private data, executable scripts or symlinks.

BendSQL extracts these resources into a private, read-only directory for an AI
session. Its prompt identifies the directory, the resource paths and bounded
UTF-8 document content so model-only HTTP backends can also use the references.
Resources are reference material: they do not grant database, filesystem or Shell
permissions and must not override the execution rules of the session.

No task-specific skills are bundled yet. Future skills must clearly distinguish
SQL suggestions from execution, and full query results from cached previews.
