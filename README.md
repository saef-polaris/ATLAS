<p align="center">
  <img src="logo.png" width="80"><br>
  <strong>POLARIS</strong><br>
  Policy Observatory and Link Analysis
</p>

ATLAS is a small toolkit for tracing how Antarctic Treaty final reports connect
**papers**, **agenda items**, and **formal outputs**.

In practice, it helps answer questions like:
- what supported a given Decision, Measure, or Resolution?
- which papers sat under a given item?
- what outputs can be linked back to a paper?

ATLAS is built as a Python package with a simple CLI.

## Data

The extraction data lives in a separate repo, `ATLAS-data`. It is not a Python
package dependency; ATLAS just reads files from that data directory.

ATLAS looks for data via:

```bash
ATLAS_DATA_DIR
```

If that is not set, it defaults to:

```bash
../ATLAS-data
```

If the data repo is missing, you can bootstrap it with:

```bash
python -m atlas.support_tracer_cli init-data
```

## Basic use

Build parser tables:

```bash
python -m atlas.parse_marker_full_pagewise
```

Export a manual-validation bundle from the rule-based links:

```bash
python -m atlas.support_tracer_cli export-validation --output-path validation_bundle.json
```

Or classify item content with Gemma (`gemma-4-26b-a4b-it`) and export an LLM validation bundle. This extracts:
- direct paper -> output links inferred from the item text

Then run:

```bash
python -m atlas.support_tracer_cli classify-items-llm --limit 30 --workers 4
```

Then open `manual_validation.html` in a browser and load the exported JSON file.
The validator shows the selected case alongside a deduplicated item-level graph:
paper(s) -> item entry/entries -> outcome(s). Click an edge to approve, reject,
correct, or skip that inference. Review downloads include
`claim_review_rows` and `edge_review_rows` so paper-to-item and
item-to-outcome validity can be reconstructed without re-opening the browser
state.

Build the backend:

```bash
python -m atlas.support_tracer_cli build-db
```

Query an output:

```bash
python -m atlas.support_tracer_cli output "Decision 1 (2004)"
```

Query an item:

```bash
python -m atlas.support_tracer_cli item ATCM27_full_pagewise 5 --sequence-type atcm
```

Query a paper:

```bash
python -m atlas.support_tracer_cli paper WP-48
```

## Python

```python
from atlas import build_or_refresh_database, query_by_output

build_or_refresh_database()
result = query_by_output("Decision 1 (2004)")
```
