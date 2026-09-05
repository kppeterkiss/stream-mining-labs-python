# PyFlink Practicals - Quick Start Guide

This guide explains how to run the Python streaming practicals on Apache Flink using PyFlink.

## Prerequisites

1. **Java Development Kit (JDK 11+)** - PyFlink requires a local JDK/JRE
   - Check: `java -version`
   - On macOS: automatically discovered via `/usr/libexec/java_home`

2. **Python 3.11** - PyFlink 1.20 is compatible with Python ≤ 3.11
   - Already provisioned in `.venv` by uv

3. **uv package manager** - for virtual environment and dependency management
   - Check: `uv --version`

## Initial Setup (One-Time)

```bash
cd /path/to/stream-mining-labs

# Set Java environment
export JAVA_HOME=$(/usr/libexec/java_home)

# Install dependencies into uv venv 
uv pip install --python .venv/bin/python setuptools wheel
uv pip install --python .venv/bin/python --no-build-isolation -r requirements.txt
```

```bash
uv init
uv venv sm-uv-venv
source sm-uv-venv/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home)
uv pip install -r requirements.txt


```

## Environment Variables (Set Before Each Session)

```bash
# From repo root: /path/to/stream-mining-labs

# Required for PyFlink Java bridge
export JAVA_HOME=$(/usr/libexec/java_home)

# Required for Python module imports
export PYTHONPATH=src
```

## Running a PyFlink Program

**General Command Patterns:**

For file/socket practicals (Bloom Filter, Count-Distinct, Count-Min Sketch):
```bash
uv run --python .venv/bin/python -- python -m MODULE_NAME --source SOURCE [OPTIONS]
```

For generator practicals (Fraud Detection, Single-Pass K-Means):
```bash
uv run --python .venv/bin/python -- python -m MODULE_NAME [OPTIONS]
```

**Breaking it down:**
- `uv run` - execute command in uv-managed environment
- `--python .venv/bin/python` - use Python 3.11 from uv venv
- `--` - separate uv args from Python args
- `python -m MODULE_NAME` - run module using Python `-m` flag (enables package imports)
- `--source {file|socket}` - source selector for file/socket practicals only
- `[OPTIONS]` - program-specific arguments (parallelism, window size, algorithm params, etc.)

---

## Practicals Overview & Commands

### 1. Bloom Filter (Duplicate Detection)

**What it does:**
- Detects duplicate strings using a probabilistic Bloom filter
- Maintains separate filter state per key (first character)
- Outputs only unique items (first occurrence within the bloom filter)

**File Source (Recommended for Testing):**

```bash
export JAVA_HOME=$(/usr/libexec/java_home)
export PYTHONPATH=src

uv run --python .venv/bin/python -- python -m stream_mining_labs_python.bloomfilter.bloom_filter_program \
  --source file \
  --input resources/inputEmails \
  --k 3 \
  --n 10
```

**Socket Source (Real-time Stream):**

Terminal A (Start data source):
```bash
# macOS/Linux: Use nc or ncat (see Socket Helper Tools section for details)
nc -l 9999          # or: nc -lk 9999 (if supported)
# or
ncat -lk 9999
```

Terminal B (Run Flink job):
```bash
export JAVA_HOME=$(/usr/libexec/java_home)
export PYTHONPATH=src

uv run --python .venv/bin/python -- python -m stream_mining_labs_python.bloomfilter.bloom_filter_program \
  --source socket \
  --host localhost \
  --port 9999 \
  --k 3 \
  --n 10
```

**Options:**
- `--k` (int, default=3) - Number of hash functions
- `--n` (int, default=10) - Bit array size
- `--parallelism` (int, default=1) - Flink parallelism degree

---

### 2. Count-Distinct (Cardinality Estimation)

**What it does:**
- Estimates the number of distinct elements in a stream
- Uses a trailing-zero cardinality estimation algorithm
- Provides approximate counts with probabilistic guarantees

**File Source (Recommended):**

```bash
export JAVA_HOME=$(/usr/libexec/java_home)
export PYTHONPATH=src

uv run --python .venv/bin/python -- python -m stream_mining_labs_python.countdistinct.count_distinct_program \
  --source file \
  --input resources/input \
  --bitmap-length 12
```

**Socket Source:**

Terminal A (Start data source):
```bash
nc -l 9998  # or: ncat -lk 9998
```

Terminal B (Run Flink job):
```bash
export JAVA_HOME=$(/usr/libexec/java_home)
export PYTHONPATH=src

uv run --python .venv/bin/python -- python -m stream_mining_labs_python.countdistinct.count_distinct_program \
  --source socket \
  --host localhost \
  --port 9998 \
  --bitmap-length 12
```

**Options:**
- `--bitmap-length` (int, default=12) - Size of bit array for cardinality sketch
- `--parallelism` (int, default=1) - Flink parallelism degree

**Output:**
```
Estimate: 2
Estimate: 4
Estimate: 8
```

---

### 3. Count-Min Sketch (Frequency Estimation)

**What it does:**
- Estimates frequency of items in a stream
- Maintains a sketch table of hash counters
- Provides approximate counts with configurable space/accuracy tradeoff

**File Source (Recommended):**

```bash
export JAVA_HOME=$(/usr/libexec/java_home)
export PYTHONPATH=src

uv run --python .venv/bin/python -- python -m stream_mining_labs_python.countminsketch.cms_program \
  --source file \
  --input resources/input \
  --rows 5 \
  --columns 12
```

**Socket Source:**

Terminal A:
```bash
nc -l 9997  # or: ncat -lk 9997
```

Terminal B:
```bash
export JAVA_HOME=$(/usr/libexec/java_home)
export PYTHONPATH=src

uv run --python .venv/bin/python -- python -m stream_mining_labs_python.countminsketch.cms_program \
  --source socket \
  --host localhost \
  --port 9997 \
  --rows 5 \
  --columns 12
```

**Options:**
- `--rows` (int, default=5) - Number of hash functions / rows in sketch
- `--columns` (int, default=12) - Width of sketch table (hash buckets)
- `--parallelism` (int, default=1) - Flink parallelism degree

---

### 4. Fraud Detection (Stateful Processing)

**What it does:**
- Detects suspicious transaction patterns using a data generator
- Maintains per-account state: last transaction amount and timer
- Rule: Small amount (< $1) followed within 1 minute by large amount (> $500) triggers alert
- Uses processing-time timers to auto-cleanup state
- **Source:** Synthetic transactions generated automatically (no file/socket input needed)
- **Current implementation detail:** Uses bounded generated data loaded with `env.from_collection(...)`

**Run the job:**

```bash
export JAVA_HOME=$(/usr/libexec/java_home)
export PYTHONPATH=src

uv run --python .venv/bin/python -- python -m stream_mining_labs_python.spendreport.fraud_detection_job \
  --records 5000
```

**With options:**
```bash
uv run --python .venv/bin/python -- python -m stream_mining_labs_python.spendreport.fraud_detection_job \
  --delay 50 \
  --seed 123 \
  --records 10000 \
  --parallelism 1
```

**Options:**
- `--delay` (float, default=100.0) - Delay between transactions in milliseconds
- `--seed` (int, default=42) - Random seed for reproducible transaction patterns
- `--records` (int, default=5000) - Number of generated transactions to process
- `--parallelism` (int, default=1) - Flink parallelism degree

**Output (Sample alerts):**
```
ALERT account_id=3
ALERT account_id=7
ALERT account_id=1
```

Note: Alerts are pattern-driven and probabilistic. For short runs, you may see few or no alerts.

---

### 5. Single-Pass K-Means (Streaming Clustering)

**What it does:**
- Performs incremental k-means clustering on a sliding window of synthetic 2D points
- Groups points into k clusters as they arrive from a data generator
- Outputs cluster summary (centroid, point count) every window
- **Source:** Synthetic points generated automatically (no file/socket input needed)
- **Current implementation detail:** Uses bounded generated data loaded with `env.from_collection(...)`

**Run the job:**

```bash
export JAVA_HOME=$(/usr/libexec/java_home)
export PYTHONPATH=src

uv run --python .venv/bin/python -- python -m stream_mining_labs_python.singlepasskmeans.kmeans_program \
  --records 500
```

**With options:**
```bash
uv run --python .venv/bin/python -- python -m stream_mining_labs_python.singlepasskmeans.kmeans_program \
  --delay 50 \
  --window 10 \
  --k 3 \
  --seed 1 \
  --records 2000 \
  --parallelism 1
```

**Options:**
- `--delay` (float, default=100.0) - Delay between points in milliseconds
- `--window` (int, default=10) - Count window size (trigger clustering every N points)
- `--k` (int, default=3) - Number of clusters
- `--seed` (int, default=1) - Random seed for point generation
- `--records` (int, default=5000) - Number of generated points to process
- `--parallelism` (int, default=1) - Flink parallelism degree

**Output:**
```
 [ cp (1.50 2.30), n 1,ls (1.50 2.30), ss (2.25 5.29)]
 [ cp (3.20 4.10), n 1,ls (3.20 4.10), ss (10.24 16.81)]
 [ cp (0.50 1.20), n 1,ls (0.50 1.20), ss (0.25 1.44)]
```

---

## Data Sources Explained

### File Source
- **Use when:** Testing, replaying data, batch-like workloads
- **Input:** Any text file (1 record per line)
- **Location:** Can be absolute or relative to repo root
- **Example files:**
  - `resources/input` - Generic test data
  - `resources/inputEmails` - Email addresses for Bloom filter

### Socket Source
- **Use when:** Real-time streaming, integration testing, production demos
- **Setup:** Run `nc` or `ncat` in separate terminal (see "Socket Helper Tools" section)
- **Send data:** Type or pipe lines (one per line, end with Enter)
- **Used by these practicals (socket sources):**
  - Bloom Filter: port 9999
  - Count-Distinct: port 9998
  - Count-Min Sketch: port 9997
- **Note:** Fraud Detection and Single-Pass K-Means use synthetic data generators (no socket needed)

### Synthetic Generator Source (Fraud + K-Means)
- **Use when:** You want runnable demos without external input tooling
- **Current Python implementation:** Data is generated in Python, materialized as a bounded list, then fed via `env.from_collection(...)`
- **Tradeoff:** This is finite by design (`--records`), unlike a true unbounded SourceFunction
- **Controls:** `--records` sets run length; `--delay` emulates arrival pacing

**Example: Send data via pipe**
```bash
cat resources/input | ncat localhost 9999
```

---

## Java SourceFunction vs Python Substitution

### Why the implementation differs

In the original Java practicals, `TransactionSource` and `PointSource` are true Java `SourceFunction`s that emit unbounded events over time.

In this PyFlink environment (1.20.5), `pyflink.datastream.functions.SourceFunction` is a Java wrapper class, not a Python subclass extension point. A direct Python subclass causes runtime errors such as:

- `AttributeError: object has no attribute '_j_function'`
- `TypeError: SourceFunction.__init__() missing 1 required positional argument: 'source_func'`

So the Python jobs use a compatible substitution:

1. Generate synthetic records in Python
2. Create stream with `env.from_collection(...)`
3. Use `--delay` to approximate streaming pace

### How to mimic original Java behavior more closely

If you need true unbounded source semantics (closer to Java):

1. Use socket ingestion for these two jobs as well
2. Run an external producer that continuously emits transactions/points
3. Keep Flink job source as socket/file compatible input stream

Alternative for production-grade parity:

1. Implement the source in Java (or use a Java connector source)
2. Expose records to Python operators downstream
3. Keep Python focused on processing logic while source runs in JVM

Practical recommendation:

1. For teaching and reproducible tests: current `from_collection` + `--records` approach
2. For lifecycle/backpressure/unbounded behavior demos: external producer + socket (or Kafka)

Approximate long-running behavior with current Python substitution:

```bash
# Fraud (long run)
uv run --python .venv/bin/python -- python -m stream_mining_labs_python.spendreport.fraud_detection_job \
  --records 2000000 --delay 10 --seed 42 --parallelism 1

# K-Means (long run)
uv run --python .venv/bin/python -- python -m stream_mining_labs_python.singlepasskmeans.kmeans_program \
  --records 2000000 --delay 10 --window 10 --k 3 --parallelism 1
```

---

## Socket Helper Tools (ncat vs nc and Platform-Specific Setup)

To send data to a PyFlink job via socket source, you need a tool that listens on a port and streams text lines. Here's a detailed comparison and setup guide.

### What Tool Should I Use?

**macOS:**
- `nc` is built-in (already installed)
- Command: `nc -l 9999` or `nc -lk 9999` (with -k for multiple clients)
- `ncat` can be installed via Homebrew but not required

**Linux:**
- `nc` is usually built-in
- `ncat` can be installed via package manager for cross-platform consistency

**Windows:**
- `ncat` must be installed separately (from nmap)
- PowerShell or Python alternatives available

### macOS Quick Start

**Using built-in `nc` (Recommended - No Installation):**

Terminal A (Listen for connections):
```bash
nc -l 9999
```

Then type lines (one per line, press Enter after each):
```
user@example.com
user2@example.com
user3@example.com
```

Terminal B (Run Flink job in parallel):
```bash
export JAVA_HOME=$(/usr/libexec/java_home)
export PYTHONPATH=src
uv run --python .venv/bin/python -- python -m stream_mining_labs_python.bloomfilter.bloom_filter_program \
  --source socket --host localhost --port 9999
```

**Piping a file through `nc`:**
```bash
# Terminal A: Listen
nc -l 9999

# Terminal B: Send file contents
cat resources/inputEmails | nc localhost 9999
```

### Linux Setup

**Using built-in `nc`:**
```bash
which nc          # Check if installed
nc -l 9999        # Listen on port 9999
```

**Installing `ncat` for consistency:**

Ubuntu/Debian:
```bash
sudo apt-get update
sudo apt-get install nmap
ncat -lk 9999
```

CentOS/RedHat:
```bash
sudo yum install nmap
ncat -lk 9999
```

Arch Linux:
```bash
sudo pacman -S nmap
ncat -lk 9999
```

### Windows Setup

**Option 1: Install ncat (Recommended)**

1. Download nmap installer from [nmap.org/download](https://nmap.org/download)
2. Run installer (choose "ncat" in components)
3. Open Command Prompt or PowerShell:
```cmd
ncat -lk 9999
```

**Option 2: PowerShell (Built-in, No Installation)**

Save this as `socket_listener.ps1`:
```powershell
$listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Parse('127.0.0.1'), 9999)
$listener.Start()
Write-Host "Listening on port 9999. Ctrl+C to stop."
while ($true) {
    $client = $listener.AcceptTcpClient()
    $stream = $client.GetStream()
    $reader = New-Object System.IO.StreamReader($stream)
    while ($client.Connected) {
        $line = $reader.ReadLine()
        if ($null -ne $line) { Write-Host "Received: $line" }
    }
    $client.Close()
}
```

Run it:
```powershell
powershell -ExecutionPolicy Bypass -File socket_listener.ps1
```

**Option 3: Use WSL (Windows Subsystem for Linux)**
```bash
wsl
sudo apt-get install nmap
ncat -lk 9999
```

**Option 4: Python (Any Platform)**

Save as `socket_server.py`:
```python
import socket

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('127.0.0.1', 9999))
server.listen(1)
print("Listening on port 9999...")

while True:
    client, addr = server.accept()
    print(f"Client connected from {addr}")
    while True:
        try:
            data = client.recv(1024).decode('utf-8')
            if not data: break
            print(f"Received: {data.strip()}")
        except: break
    client.close()
```

Run:
```bash
python socket_server.py
```

### Understanding ncat vs nc

**`ncat` (netcat from nmap suite):**
- Modern, actively maintained
- `-lk` keeps listening after client disconnects (accept multiple)
- Cross-platform: macOS, Linux, Windows
- Best for Flink practicals (handles multiple connections)

**`nc` (BSD netcat, built-in on Unix):**
- Older, simpler implementation
- Varies by OS: `-k` flag may not exist on all versions
- Built-in on macOS and Linux (no install needed)
- Works fine for single connections

**Practical difference:**
```bash
# ncat (recommended)
ncat -lk 9999         # -k keeps listening; works cross-platform

# nc (works on macOS/Linux, may vary)
nc -l 9999            # Listen (single connection)
nc -lk 9999           # Listen (multiple connections, if -k supported)
```

### Common Usage Patterns

**Pattern 1: Type data interactively**
```bash
nc -l 9999           # Terminal A: Listen
# Type lines as needed, press Enter after each
```

**Pattern 2: Stream a file**
```bash
nc -l 9999                                        # Terminal A
cat resources/input | nc localhost 9999  # Terminal B
```

**Pattern 3: Stream with delay (1 line per second)**
```bash
nc -l 9999                                                      # Terminal A
while read line; do echo "$line"; sleep 1; done < data.txt | nc localhost 9999  # Terminal B
```

**Pattern 4: Generate and stream numbers**
```bash
nc -l 9999              # Terminal A
seq 1 1000 | nc localhost 9999  # Terminal B: Send 1-1000
```

### Troubleshooting

**"Address already in use" error**
```bash
# Wait 30-60 seconds or use different port
nc -l 9998  # Try port 9998 instead

# Find and kill process (macOS/Linux)
lsof -i :9999
kill -9 <PID>
```

**"Connection refused" in Flink job**
- Start socket listener BEFORE running the job
- Verify port number matches (9999, 9998, etc.)
- Check firewall allows connections

**Data not flowing**
```bash
# IMPORTANT: Press Enter after each line
nc -l 9999
user@example.com
<ENTER>  # Required!
user2@example.com
<ENTER>  # Required!
```

**"ncat: command not found" or "nc: command not found"**
- Use Python socket server (works everywhere)
- Install nmap (includes ncat)
- Check `which nc` and `which ncat` to see what's available

---

## Debugging & Troubleshooting

### Error: `ModuleNotFoundError: No module named 'stream_mining'`
**Cause:** Missing PYTHONPATH or wrong working directory
**Fix:**
```bash
export PYTHONPATH=src
cd /path/to/stream-mining-labs  # Ensure you're at repo root
```

### Error: `AttributeError: 'StreamExecutionEnvironment' object has no attribute 'socket_text_stream'`
**Cause:** Older PyFlink API version
**Fix:** Uses automatic fallback via `flink_compat.py` - no action needed

### Error: `TypeError: SourceFunction.__init__() missing 1 required positional argument: 'source_func'`
**Cause:** In this PyFlink version, `SourceFunction` is a Java wrapper and cannot be subclassed directly in Python for custom emit loops.
**Fix:** Use the current substitution in this repo (`env.from_collection(...)` + generated records), or move to a Java source / external socket producer for unbounded behavior.

### Error: `AttributeError: object has no attribute '_j_function'`
**Cause:** Same SourceFunction API mismatch as above (Python class did not provide a backing Java function object).
**Fix:** Same as above: use collection-based substitution or a JVM-backed/external source.

### Error: `java.net.ConnectException: Connection refused`
**Cause:** Socket source port is not listening
**Fix:** Start `ncat -lk PORT` before running the job

### Error: `org.apache.flink.runtime.JobExecutionException`
**Cause:** Job logic error or data parsing failure
**Fix:** Check input data format, increase verbosity with `--parallelism 1`

### Warnings about `pkg_resources` deprecated
**Status:** Safe to ignore - from Apache Beam dependency, doesn't affect functionality

---

## Environment Setup Script (Optional)

Save as `setup_env.sh`:

```bash
#!/bin/bash
export JAVA_HOME=$(/usr/libexec/java_home)
export PYTHONPATH=src
echo "✓ JAVA_HOME=$JAVA_HOME"
echo "✓ PYTHONPATH=$PYTHONPATH"
```

Then use:
```bash
source setup_env.sh
uv run --python .venv/bin/python -- python -m stream_mining_labs_python.bloomfilter.bloom_filter_program --help
```

---

## What Each Practical Teaches

| Practical | Concept | Difficulty | Data Structure |
|-----------|---------|-----------|-----------------|
| Bloom Filter | Probabilistic membership test | ⭐ | Bit array + hash functions |
| Count-Distinct | Cardinality estimation | ⭐⭐ | Bit array + trailing zeros |
| Count-Min Sketch | Frequency estimation | ⭐⭐ | 2D hash table |
| Fraud Detection | Stateful stream processing | ⭐⭐⭐ | Key-value state + timers |
| K-Means | Incremental clustering | ⭐⭐⭐ | Clustering features |

---

## Advanced Options

### Increase Parallelism (Multi-threaded Flink)
```bash
uv run --python .venv/bin/python -- python -m stream_mining_labs_python.bloomfilter.bloom_filter_program \
  --source file \
  --input resources/inputEmails \
  --parallelism 4
```

### Change Algorithm Parameters (Bloom Filter)
```bash
# More accurate: larger n (bit array), more k (hash functions)
uv run --python .venv/bin/python -- python -m stream_mining_labs_python.bloomfilter.bloom_filter_program \
  --source file \
  --input resources/inputEmails \
  --k 5 \
  --n 20
```

### Change Window Size (K-Means)
```bash
# Cluster every 50 points instead of 10
uv run --python .venv/bin/python -- python -m stream_mining_labs_python.singlepasskmeans.kmeans_program \
  --source file \
  --input resources/input \
  --window 50
```

---

## Key Files & Structure

```
stream-mining-labs/
├── src/
│   └── stream_mining/
│   │   ├── flink_compat.py                    # Socket/file source compatibility helper
│   │   ├── bloomfilter/
│   │   │   ├── bloom_filter_program.py        # Entry point (PyFlink job)
│   │   │   └── bloom_filter_keyed.py          # Algorithm logic
│   │   ├── countdistinct/
│   │   │   ├── count_distinct_program.py      # Entry point (PyFlink job)
│   │   │   └── cont_distinct.py               # Algorithm logic
│   │   ├── countminsketch/
│   │   │   ├── cms_program.py                 # Entry point (PyFlink job)
│   │   │   └── cms.py                         # Algorithm logic
│   │   ├── spendreport/
│   │   │   ├── fraud_detection_job.py         # Entry point (PyFlink job with timers)
│   │   │   └── fraud_detector.py              # Algorithm logic
│   │   ├── singlepasskmeans/
│   │   │   ├── kmeans_program.py              # Entry point (PyFlink windowed job)
│   │   │   ├── single_pass_kmeans1.py         # Algorithm logic
│   │   │   └── util/                          # Helper classes (Point, ClusteringFeature)
│   │   └── utilities/                         # Shared (bitmap, hash functions)
├── requirements.txt                           # Dependencies (PyFlink, river, etc.)
├── resources/
│   ├── input                                  # Generic test data
│   ├── inputEmails                            # Email addresses
└── .venv/                                     # Local virtual environment (Python 3.11)
```

---

## Next Steps

1. **Start with Bloom Filter** (simplest)
   ```bash
   export JAVA_HOME=$(/usr/libexec/java_home)
   export PYTHONPATH=src
   uv run --python .venv/bin/python -- python -m stream_mining_labs_python.bloomfilter.bloom_filter_program --source file --input resources/inputEmails
   ```

2. **Try socket sources** to see real-time streaming in action

3. **Experiment with parameters** (--k, --n, --window, --parallelism)

4. **Examine the algorithm classes** in `src/stream_mining_labs_python/MODULE/` to understand the logic

---

## More Information

- [Apache Flink Documentation](https://flink.apache.org/docs/stable/)
- [PyFlink API Reference](https://nightlies.apache.org/flink/flink-docs-master/api/python/)
- [Stream Processing Patterns](https://flink.apache.org/features/stream-processing/)
