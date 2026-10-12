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

The single built-in skill is `databend/SKILL.md`: official documentation pointers,
EXPLAIN / EXPLAIN ANALYZE / GRAPHICAL guidance and common query-troubleshooting FAQ.
It distinguishes suggestions from execution and complete results from previews.
Keep each skill document within the 4 KiB prompt excerpt budget; resources do not
provide a browser, SQL or Shell tool and do not fetch documentation automatically.
