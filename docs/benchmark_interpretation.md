# Storage Benchmark Interpretation

Results from `data/benchmarks/benchmark_results.csv` (49,897 curated rows,
5 repeated measurements, median reported, filter = `status = 'DELIVERED'`).

| storage_type | file_size_bytes | write_s | full_read_s | filtered_read_s |
|---|---|---|---|---|
| csv | 15,097,146 | 0.820 | 0.211 | 0.238 |
| json_lines | 30,839,534 | 0.647 | 0.561 | 0.272 |
| parquet | 5,456,487 | 0.099 | 0.053 | 0.029 |
| postgresql | 16,564,224 | n/a | 0.334 | 0.056 |

## 1. Which file format was smallest on disk, and why?

Parquet, by roughly a 3x margin over CSV and 5.6x over JSON Lines. Parquet
stores data column-by-column with a typed, binary encoding and applies
compression per column; a column of mostly-repeated strings (`status`,
`category`) or numerics with low entropy compresses very well. CSV and JSON
are both row-oriented, uncompressed text, and JSON additionally repeats
every column name on every single row, which is why it is roughly double
the size of CSV for the same logical data.

## 2. Which representation was fastest for a full dataset read? Does that imply it is best for every workload?

Parquet was fastest for a full read (0.053s) by a wide margin, mainly because
it is smaller (less I/O) and pyarrow reads its typed binary layout directly
into columnar arrays with no per-row text parsing. That does not make it
best for *every* workload: a system that only ever appends one JSON event
at a time and never rewrites history (e.g. a log shipper) is easier to
build correctly on JSON Lines, since Parquet files are not designed for
single-row appends. A human who needs to `diff` two versions of a dataset
in git-style tooling, or open it directly in a text editor, is better served
by CSV. Parquet wins specifically for analytical, columnar, read-heavy
workloads like this one.

## 3. How did filtered retrieval differ between Parquet and PostgreSQL? What additional PostgreSQL design could change the result?

Parquet's filtered read (0.029s) used `pyarrow.parquet.read_table(..., filters=[...])`,
which applies predicate pushdown against Parquet's per-row-group statistics --
it can skip whole row groups that cannot contain `status='DELIVERED'` without
decoding them. PostgreSQL's filtered read (0.056s) was a full sequential scan
of `curated.sales_order_lines`, since no index exists on `status`. Adding
`CREATE INDEX idx_sales_order_lines_status ON curated.sales_order_lines(status);`
(or a partial index `WHERE status = 'DELIVERED'` if that single value is
queried disproportionately often) would let PostgreSQL use an index scan
instead of a sequential scan, which should bring its filtered-read time much
closer to, or below, Parquet's -- at the cost of extra storage and slower
writes/UPSERTs, since every insert/update must also maintain the index.

## 4. Why is JSON Lines generally more pipeline-friendly than one giant JSON array for append/stream-oriented processing?

A single JSON array requires the writer to hold the entire array (or at
least its closing `]`) and requires a reader to parse the whole structure
(or use a streaming JSON parser with nontrivial state tracking) before it
can safely consume any one record, because a truncated array is invalid
JSON. JSON Lines encodes each record as a complete, independent JSON value
on its own line: a writer can `fsync` and append one line at a time without
ever touching what came before, and a reader can process and discard each
line as it arrives, tolerate a truncated final line from a crashed writer,
and parallelize processing across lines trivially. That is exactly the
"stream-oriented" property array-of-JSON lacks.

## 5. What happens if a partition key has extremely high cardinality, or poor query locality?

`order_year`/`order_month` here produces on the order of dozens of
partitions for a multi-year dataset -- coarse enough that each partition
directory holds a meaningful number of rows. If the partition key instead
had extremely high cardinality (e.g. partitioning by `order_id` itself, or
by a UUID), the result would be a huge number of tiny partition directories:
metadata overhead (one Parquet file, its footer, and a directory entry per
partition) would dominate actual data volume, "small file" overhead would
slow down any engine that has to open thousands of files to satisfy a query,
and most queries -- which rarely filter on a key that granular -- would
gain no pruning benefit at all, since the ability to skip a partition
depends on the query's filter aligning with the partition key ("query
locality"). A key like `customer_id` for a query pattern that mostly filters
by date would suffer the same problem: partitioning must match how the data
is actually queried, or it adds overhead without adding any prune benefit.
