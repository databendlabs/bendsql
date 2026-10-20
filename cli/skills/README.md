# BendSQL embedded skills

This directory holds trusted, versioned text resources bundled into the BendSQL
binary in both debug and release builds. Add a skill under `<name>/SKILL.md`, with
optional Markdown reference documents alongside it. Do not add credentials,
private data, executable scripts or symlinks.

BendSQL supplies resource names and bounded UTF-8 Markdown content directly from
compiled bytes. There is no extraction directory or local path for the model to
read. Only SKILL.md subtrees enter the prompt; this maintainer README does not.
Resources are reference material: they do not grant database, filesystem or Shell
permissions and must not override the execution rules of the session.

The single built-in skill is `databend/SKILL.md`: official documentation pointers,
EXPLAIN / EXPLAIN ANALYZE / GRAPHICAL guidance and common query-troubleshooting FAQ.
It distinguishes suggestions from execution and complete results from previews.
Keep each skill document within the 4 KiB prompt excerpt budget; resources do not
provide a browser, SQL or Shell tool and do not fetch documentation automatically.
