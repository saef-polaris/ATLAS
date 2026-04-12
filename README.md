# ATLAS

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
