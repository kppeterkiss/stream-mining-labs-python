# stream_mining Python port

This folder contains a Python conversion of the Java practical materials.
The runnable programs now target an Apache Flink backend through PyFlink.

**For detailed command examples and explanations, see [PYFLINK_QUICKSTART.md](PYFLINK_QUICKSTART.md).**

## Structure

- stream_mining/bloomfilter: Bloom filter examples
- stream_mining/countdistinct: count-distinct sketch example
- stream_mining/countminsketch: count-min sketch example
- stream_mining/moaclassifier: online classifier training/testing example
- stream_mining/singlepasskmeans: single-pass k-means practical
- stream_mining/spendreport: fraud-detection practical
- stream_mining/utilities: shared bitmap/hash utilities

## Setup

```bash
cd /path/to/stream-mining-labs-python
make bootstrap
source scripts/setup_env.sh
```

Manual equivalent:

```bash
cd /path/to/stream-mining-labs-python
uv venv --python 3.11 .venv
uv sync
source scripts/setup_env.sh
```

PyFlink support usually tracks Python <= 3.11. If your default interpreter is newer,
create the virtual environment with Python 3.11 as above.

PyFlink requires a local JDK/JRE and JAVA_HOME to be set.

## Directory Structure

```
src/stream_mining/
  ├── bloomfilter/            # Bloom filter duplicate detection
  ├── countdistinct/          # Cardinality estimation
  ├── countminsketch/         # Frequency estimation (CMS)
  ├── moaclassifier/          # Online learning classifier
  ├── singlepasskmeans/       # Streaming k-means clustering
  ├── spendreport/            # Fraud detection with timers
  ├── utilities/              # Shared bitmap and hash utilities
  ├── flink_compat.py         # Socket/file source compatibility helper
  └── <module>_program.py     # PyFlink entry point for each practical
```

## Quick Start

See [PYFLINK_QUICKSTART.md](PYFLINK_QUICKSTART.md) for:
- Detailed explanations of each practical
- Socket vs file source examples
- How to send live data via ncat
- Algorithm parameters and options
- Debugging tips

**Example (Count-Distinct from file):**

```bash
cd /path/to/stream-mining-labs-python
export JAVA_HOME=$(/usr/libexec/java_home)
export PYTHONPATH=src
uv run --python .venv/bin/python -- python -m stream_mining_labs_python.countdistinct.count_distinct_program --source file --input resources/input
```

## Notes

- The conversion preserves algorithmic behavior and educational intent from Java source.
- Core practical jobs execute on PyFlink DataStream runtime.
- Modules with `_program.py` suffix are PyFlink streaming jobs.
- `moaclassifier/main.py` is currently standalone Python (river-based online learning).
