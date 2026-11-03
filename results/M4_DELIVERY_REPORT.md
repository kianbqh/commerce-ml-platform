# M4 delivery report: checked arrival replay and local Spark

I extended the historical Instacart warehouse to study ordered incremental
loading, corrections, blocking checks and exact Spark read-back. The parent
data run, selected training/HTTP child and private/public dashboard views have
completed and passed independent review.

## Source, execution and evidence

| Item | Identity |
|---|---|
| Parent source | `829250d93e5e436e98e4a5df41296b0d121a52a8` |
| Parent Dagster run | `913dbb54-c51d-49bf-a225-3741fe84d2d1` |
| Parent canonical config identity | `ebcb8f3d544778e2f9da6c253a98441f02b0ed2792955564f197cc42b54f515e` |
| Arrival manifest canonical identity | `ac1322e7f9f03feef80620bc71c41ab3cc3834c6bb90f27ea1a5c6e85dbeea24` |
| Accepted private parent review SHA256 | `25bcd2a251fcf807e763c6aad5e07c612a829598f72267b711fb97c5752dae9a` |
| Checked PR source and CI | `f6572128dedbf03daebdb1fb398aa79f1dd5e8ce`; [PR 1](https://github.com/kianbqh/commerce-ml-platform/pull/1); [CI 41601768167](https://github.com/kianbqh/commerce-ml-platform/actions/runs/41601768167), five jobs success |

[Summary](orchestration/full-20261103/summary.json) · [aggregate counts](orchestration/full-20261103/aggregate-counts.json) · [parent supplement](orchestration/full-20261103/parent-supplement.json) · [per-batch CSV](orchestration/full-20261103/build-timings.csv).
These public artifacts contain aggregate values, protocol and logical hashes.
The original CSV/DB/Parquet and row-level private evidence stay local. Historical
inputs are used under the [Dataset Terms](https://gist.github.com/jeremystan/582eba13d6ee27ed465c43dc78934700);
historical rows and real subsets are not redistributed. CI constructs independent
synthetic baskets.

## Environment and protocol

The parent ran on WSL2, Python 3.12.3 and an Intel i7-12700KF with 20 logical
CPUs. Dagster 1.13.28, dagster-dbt 0.29.28, dbt-core 1.12.6,
dbt-duckdb 1.11.1, DuckDB 2.0.0, NumPy 2.5.4 and PySpark 4.2.0 are recorded.
dbt/audits and the terminal feature exporter used 4 threads/4 GB.
Spark actually used `local[8]`, parallelism 8, with a `4g` driver.
This is one declared repetition with elapsed process timers.

The parent environment intentionally has Torch, FAISS, LightGBM and FastAPI
recorded as `not installed`. It performs data work only. The selected child uses
an actual separately identified model interpreter and records its own packages;
no parent absence marker is treated as a package version.

Instacart supplies order numbers and elapsed-day offsets rather than calendar
arrival timestamps. A batch is `(order_number - 1) // 10`. Daily receipt dates,
a delayed receipt and corrections are simulated. All execution is manual;
zero scheduled jobs or daemon processes were activated.

## Data and arrival batches

| Quantity | Accepted count |
|---|---:|
| Raw metadata orders, including unlabeled baskets | 3,421,083 |
| Orders with labeled lines | 3,346,083 |
| Labeled order lines | 33,819,106 |
| Users represented by labeled lines | 206,209 |
| Catalog products | 49,688 |
| Products actually observed in labeled lines | 49,685 |

The 49,688-product catalog and 49,685 observed products are different counts.
The earlier completed recommendation Results block is retained unchanged.

| Batch | Metadata orders | Labeled lines | ETL + tests (s) | Exact quality audit (s) | Partition digests (s) |
|---|---:|---:|---:|---:|---:|
| 000000 | 1,680,424 | 16,474,179 | 44.000958 | 269.286142 | 119.634277 |
| 000001 | 745,873 | 7,398,009 | 31.287910 | 504.598903 | 172.092273 |
| 000002 | 397,357 | 4,002,535 | 33.419416 | 474.432483 | 201.938139 |
| 000003 | 235,636 | 2,406,832 | 35.263382 | 523.601739 | 219.165560 |
| 000004 | 144,923 | 1,473,736 | 32.446031 | 569.806774 | 230.944803 |
| 000005 | 87,093 | 858,737 | 33.411835 | 693.418074 | 235.269370 |
| 000006 | 54,051 | 515,437 | 32.562673 | 654.449228 | 242.890389 |
| 000007 | 35,611 | 331,918 | 33.091054 | 657.076936 | 242.020876 |
| 000008 | 23,829 | 216,171 | 33.820004 | 702.046378 | 249.025791 |
| 000009 | 16,286 | 141,552 | 32.790724 | 699.694366 | 243.262134 |

## Timers and the negative performance result

| Scope | Seconds |
|---|---:|
| Single full-refresh dbt build + tests | 31.870717 |
| First ten ordered dbt ETL/test processes, sum | 342.093988 |
| Exact source/fact quality audits, sum | 5,748.411023 |
| Per-partition digests, sum | 2,156.243612 |
| Candidate copy/setup, sum | 3.078851 |
| Initial source hashing | 0.409711 |
| Initial metadata + hashing, including the preceding hash timer | 3.052664 |
| Original six-table comparison | 610.339290 |
| Corrected six-table comparison | 680.910781 |
| Clean recovery six-table comparison | 674.187822 |
| Terminal feature export | 11.029079 |
| Repeated-batch child, including its audits | 931.490509 |

The incremental ETL/test sum is **10.734×** the single full refresh;
this run showed no incremental ETL speedup. Cumulative processing and exact
audits dominate the record. ETL/test timers include process startup and log
writes; quality checks, typed digests, setup and exports are separate.
The repeated batch and correction attempts are excluded from the first-arrival
ETL sum. These scope-specific totals are not the whole pipeline's wall time.

## Exactness, correction and recovery

| Named DB table | Rows | EXCEPT ALL left / right | Full typed records/schema/key |
|---|---:|---:|---|
| `marts.fct_order_lines` | 33,819,106 | 0 / 0 | equal |
| `marts.fct_orders` | 3,346,083 | 0 / 0 | equal |
| `marts.mart_department_kpis` | 21 | 0 / 0 | equal |
| `orchestration.batch_department_integer_stats` | 210 | 0 / 0 | equal |
| `orchestration.batch_hourly_integer_stats` | 1,678 | 0 / 0 | equal |
| `orchestration.batch_product_integer_stats` | 310,819 | 0 / 0 | equal |

All six tables match in the original, corrected and recovered comparisons.
The versioned `m4-typed-framed-v1` encoding covers nulls, UTF-8 strings, exact
integers and binary64 bits. Other operational relations are built/tested by dbt
but are outside this six-table equality claim. Full digests are in the summary.

Each original arrival ran 22 input tests and 44 post-mart tests. Batch
`000009` was repeated; the before/after digests of five partition-scoped tables
across ten batches were unchanged. The repeat took 931.490509 s,
including its audits, not another contribution to the ETL total above.

Corrections changed existing `reordered` values while preserving the line key.
The first corrected attempt saw one received correction; the next saw two.
Only `000000`/`000001` changed, while eight other batch digests stayed equal.
Both facts and the integer marts reflected the change, and the corrected state
matched its own full refresh. Those two correction children took
1,056.880098 s and
1,058.376850 s; the corrected full
reference's ETL/tests took 46.403230 s.

The injected unknown product caused one actual product FK failure and the
input-integrity failure. One Dagster input-quality check failed. Only ingest
and staging materialized; downstream marts and promotion were blocked and the
promoted pointer stayed unchanged. A fresh clean attempt recovered, retaining
the failed artifacts. The local alert count is one, sensor RunRequests zero,
and no external message was sent.

Controlled clocks produced a passing fresh case and failing stale case.
Processing freshness concerns materialization age plus replay watermark,
not the age of real grocery events.

## Terminal recommendation boundary

All nine nonterminal original prefixes were verified to contain no recommendation
relations or feature export. The complete original branch passed the terminal
gate and exported features in 11.029079 s.

- Split SHA256: `b3d8bbef2396dd302465ed4511bf36fce55dd151dd7914f99aa69acd6ea9994b`.
- Feature fingerprint: `936de0e402c75b6438fc68ea906f1c66235ddf15b490cbe6527bca3271530011`.
- Feature manifest SHA256: `97e9d671c57f1c89c14e15d07f6292379c42f5843d3c47af62cf164360b8508d`.

These match the frozen feature contract. Corrected/bad branches do not become
model inputs. The parent model/API receipts remain `not run`.

## Physical Spark Parquet and read-back

Spark writes order lines and department/hourly/product integer statistics,
partitioned by replay batch. Its four outputs are narrower than the six-table
DB comparison above.

| Output | Rows | Logical nonempty/declared batches | Original files / bytes | Backfill new files / bytes |
|---|---:|---:|---:|---:|
| `fct_order_lines` | 33,819,106 | 10 / 10 | 160 / 252,345,948 | 32 / 169,067,940 |
| `batch_department_integer_stats` | 210 | 10 / 10 | 119 / 222,563 | 23 / 43,100 |
| `batch_hourly_integer_stats` | 1,678 | 10 / 10 | 160 / 298,259 | 32 / 59,959 |
| `batch_product_integer_stats` | 310,819 | 10 / 10 | 160 / 2,959,850 | 32 / 985,863 |

Original total: 599 Parquet files,
255,826,620 bytes; corrected new writes:
119 files,
170,156,862 bytes. Metadata/CRC/SUCCESS bytes
are separate. Backfill newly writes two selected batches, while composite
references retain the other eight. Acceptance independently rehashed all
599 original Parquet files without a change.

| Spark scope | Original (s) | Corrected (s) |
|---|---:|---:|
| Processing, excluding physical hashes | 150.113704 | 109.407346 |
| Physical hashing | 0.204950 | 0.111704 |
| Process including physical hashes | 150.318653 | 109.519048 |
| Source hashing, excluded from process | 1.866623 | 1.164528 |
| DuckDB exact read-back | 1,225.006004 | 1,188.500498 |

Both passes match schema, every row/value, complete keys, bidirectional
`EXCEPT ALL` and whole/per-batch typed hashes. Read-back validation costs more
than Spark processing in both passes. Selected writes still validate the whole
source; they do not imply selected-only input I/O or cluster scaling.

## Selected training/HTTP child and dashboard

The append-only child at source `f6572128dedbf03daebdb1fb398aa79f1dd5e8ce` ran
as `5b083057-a84b-4c0a-b471-bec98aa04f12`. Its actual effective config identity is
`60104ff24c4b391d09b6a64b234faac226efacb744dc2c0c2a3550b6b698da32`. The same two real Dagster assets,
`recsys_train_eval` and `api_benchmark`, materialized exactly once. The accepted
original parent DB/cache and all twelve tracked small receipts stayed unchanged.
The data replay, dbt/Spark work and terminal feature export were not repeated.

[Child summary](orchestration/full-20261103/downstream/summary.json) · [all user-ID-free HTTP timing samples](orchestration/full-20261103/downstream/http-timings.json) · [extraction/source identities](orchestration/full-20261103/downstream/extraction-manifest.json).
The linked row-free artifacts retain the measured numbers and logical source
hashes; they contain no customer/product/order rows or response content.

### Environment, split and training

The model interpreter used Python 3.12.3, imported Torch 2.15.0+cu132,
NumPy 2.5.4, FAISS 1.15.1 and LightGBM 4.7.0. All three fits recorded
`device=cpu` and four threads. FastAPI 0.162.0, Uvicorn 0.54.0 and HTTPX 0.28.1
were actually imported. The isolated orchestration environment remains separate.

Every seed used 29,524,435 training positives for each of three epochs.
Validation has 2,129,254 lines and test 2,165,417 lines across 206,209 baskets each.
There are 75,000 unlabeled metadata orders and zero excluded labeled users.
The split and feature fingerprints equal the accepted terminal baseline above.
Priors/normalizers remain training-only; test history includes each user's
completed validation basket. Reorders are eligible. No seed/config sweep was run.

| Seed | Epoch 1 loss | Epoch 2 loss | Epoch 3 loss | Positives every epoch |
|---:|---:|---:|---:|---:|
| 42 | 0.386094 | 0.322756 | 0.308429 | 29,524,435 |
| 43 | 0.386820 | 0.322551 | 0.308109 | 29,524,435 |
| 44 | 0.386151 | 0.322916 | 0.308502 | 29,524,435 |


Initial/final learned weights differ for every seed; all three final state
hashes and model IDs are distinct. Large checkpoints remain local; these are
weight-state identities rather than download/checkpoint-file hashes.

| Seed | Model ID | Final learned-weight state SHA256 |
|---:|---|---|
| 42 | `c9f3f9052ae6a10793dc0ed2887a1f37e517079b7ee49aeca60e3f2ecfac6106` | `7e9939c9bbf90ffe439770c93dab802e60e4ded7ce3c3e14728d63765890e373` |
| 43 | `05960fa821fd8131c34a4516d1dccda9ca3f52507b7a7444690537582fd8e139` | `92e1149270b2ff9fb9d528b83ae5e105408c603f26fdc64a4890ca4b40d906d0` |
| 44 | `7c7beaa66662125342102e1cdb54348490f0a41b273d29b79e74d0a077289747` | `d9711a247ba74e929a56c4025ddbc78d03011442ef4dd45dc5eff2f0e359f187` |


### Full held-out quality

The following mean ± population standard deviations use the three fixed model
seeds (42/43/44), not a confidence interval or a per-user standard deviation.
All eligible held-out users are evaluated.

| Method | NDCG@10 | Recall@10 | Micro Recall@10 | Candidate Recall@100 | NDCG@100 | Micro Recall@100 |
|---|---:|---:|---:|---:|---:|---:|
| Training popularity | 0.097616 ± 0.000000 | 0.069942 ± 0.000000 | 0.068671 ± 0.000000 | 0.217538 ± 0.000000 | 0.141229 ± 0.000000 | 0.224331 ± 0.000000 |
| Two-tower + HNSW | 0.099415 ± 0.000640 | 0.085524 ± 0.000573 | 0.072446 ± 0.000357 | 0.316579 ± 0.000528 | 0.175068 ± 0.000546 | 0.289082 ± 0.000416 |
| Two-tower + LambdaMART | 0.302808 ± 0.000473 | 0.222243 ± 0.000454 | 0.186521 ± 0.000278 | 0.316579 ± 0.000528 | 0.303029 ± 0.000447 | 0.289082 ± 0.000416 |

| Seed | Method | NDCG@10 | Recall@10 | Micro Recall@10 | Candidate Recall@100 | NDCG@100 | Micro Recall@100 |
|---:|---|---:|---:|---:|---:|---:|---:|
| 42 | Training popularity | 0.097616 | 0.069942 | 0.068671 | 0.217538 | 0.141229 | 0.224331 |
| 42 | Two-tower + HNSW | 0.100233 | 0.086113 | 0.072901 | 0.317300 | 0.175801 | 0.289665 |
| 42 | Two-tower + LambdaMART | 0.303362 | 0.222765 | 0.186868 | 0.317300 | 0.303620 | 0.289665 |
| 43 | Training popularity | 0.097616 | 0.069942 | 0.068671 | 0.217538 | 0.141229 | 0.224331 |
| 43 | Two-tower + HNSW | 0.099342 | 0.085710 | 0.072408 | 0.316384 | 0.174912 | 0.288861 |
| 43 | Two-tower + LambdaMART | 0.302854 | 0.222307 | 0.186505 | 0.316384 | 0.302924 | 0.288861 |
| 44 | Training popularity | 0.097616 | 0.069942 | 0.068671 | 0.217538 | 0.141229 | 0.224331 |
| 44 | Two-tower + HNSW | 0.098670 | 0.084748 | 0.072029 | 0.316052 | 0.174492 | 0.288721 |
| 44 | Two-tower + LambdaMART | 0.302208 | 0.221658 | 0.186189 | 0.316052 | 0.302541 | 0.288721 |


Recall divides by each complete held-out basket; NDCG uses ideal length
`min(k, basket size)`, with macro averaging over users. Micro Recall is shown
separately. Candidate coverage is not the final reranked top-ten metric.

| Seed | Fit / early-stop groups | Validation candidate macro / micro Recall@100 | Selected trees | Exact ANN top-100 agreement |
|---:|---:|---:|---:|---:|
| 42 | 185,588 / 20,621 | 0.316237 / 0.288103 | 117 | 0.984535 |
| 43 | 185,588 / 20,621 | 0.315476 / 0.287228 | 97 | 0.986460 |
| 44 | 185,588 / 20,621 | 0.314355 / 0.287070 | 120 | 0.985755 |


The ANN agreement diagnostic uses a seeded 2,000-user subset per seed; the quality
table uses all 206,209 users. Ranker fit and early stopping use disjoint validation
user groups. Seed 44 selected the full 120-round cap; seeds 42/43 selected
117/97. These are the fixed protocol's outcomes, with no later retuning.

### CPU elapsed records

| Seed | Two-tower fit (s) | Ranker features (s) | Ranker fit (s) | Full quality eval (s) | Pipeline (s) | Process (s) |
|---:|---:|---:|---:|---:|---:|---:|
| 42 | 282.173343 | 37.307958 | 73.144887 | 42.101076 | 447.780150 | 450.408158 |
| 43 | 278.291770 | 40.709722 | 68.208244 | 39.801973 | 429.393622 | 432.431681 |
| 44 | 278.120801 | 26.617806 | 72.491594 | 42.103841 | 421.285073 | 432.815331 |


The actual child took 1,551.094560 s from
`complete()` entry through post-run input verification. Its model import preflight
process took 58.425323 s.
Per-seed process timers include startup and log writes; pipeline timers retain
their original narrower scope. These are elapsed execution records, not a
controlled speed comparison with the older experiment.

### Actual HTTP and lifecycle

Seed 42 was predeclared for serving; request sampling used seed 0. One worker
served 5,000/5,000 valid HTTP200 responses, zero errors, with concurrency 8 after
50 excluded warmups. The handler retrieves, builds features, reranks and creates
items. Client time ends only after the full response body arrives.

| Timer | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|
| Client | 52.759895 | 202.528091 | 375.415107 |
| Handler | 0.770786 | 2.544492 | 3.353868 |

Measured closed-loop load: 48.018628 s,
104.126258 requests/s. Benchmark process:
49.660028 s; owned API lifecycle:
52.417082 s. The owned service terminated.
The full timing sample export retains all 5,000 original measurements while
omitting user IDs and response items. Client and handler tail values describe
their separate timer scopes.

### Actual private and public browser acceptance

[Dashboard](orchestration/full-20261103/dashboard/index.html) · [snapshot](orchestration/full-20261103/dashboard/snapshot.json) · [browser checks](orchestration/full-20261103/dashboard/browser-validation.json) · [desktop](orchestration/full-20261103/dashboard/desktop.png) · [narrow](orchestration/full-20261103/dashboard/narrow.png).
Windows headless Chromium 156.0.8078.4 checked the exact private combined view
and the sanitized public view. Both passed at 1440×1000 and 390×844: all rendered
values/statuses, visible parent/child IDs, refresh, actual JSON-link export,
zero page/console errors, zero external request attempts and no whole-page
horizontal overflow. Owned contexts/servers closed and their ports refused
connections afterward. Both real screenshot pairs were independently reviewed.

The public derivation changed 154 declared metadata pointers only, retaining all
display numbers, dates, seeds and control-run IDs. It removes native paths and
replaces raw alert reason text with a reviewed check label plus the original
reason hash. Source entries retain safe relative evidence identities and hashes
without pretending private payloads are downloadable public links.
Public snapshot SHA256:
`9919eac4d21336ba6109126dfa607aa78bcd546adfbd596246d29686e95d0edd`.
The first private launch was denied loopback access before UI checks; the
unchanged driver then passed with the required localhost access. That failed
attempt remains private and is separate from the measured data/model results.

## CI and scope

[CI 41601768167](https://github.com/kianbqh/commerce-ml-platform/actions/runs/41601768167)
passed all five jobs at API head `f6572128`, with the actual PR merge checkout
`702004298bf0599b1e178d0a0efb1fcc54c3240b` retained separately.
Warehouse/recommendation/experiments remain, alongside orchestration-controls
and the synthetic fixture job. The latter ran 181 invented lines in three
batches, actual correction/FK-block/recovery/Spark checks, three one-epoch CPU
fits and 20 HTTP responses after 50 warmups. The offline artifact audit passed
41/41 checks. These are toy correctness results, not full-data quality or speed.

This covers static historical data, simulated ordered arrivals/corrections,
one-machine DuckDB/local Spark and local alerts. It does not measure live
production CDC, real event-time freshness, distributed scaling or online
recommendation impact. Row deletes and primary-key changes are outside the
correction protocol. Hillstrom remains a separate campaign experiment.
GCP/BigQuery/Cloud Run was not applied; an optional cloud run would need a
separate supplied project, not another input for this local result.

## Preserved earlier results

The original [Results block](../README.md#results), linked earlier metrics/HTTP
artifacts and MovieLens archive remain byte-identical and separate from this
new parent/child experiment. Its required block SHA256 is
`0fb161d95da9a973a23ee305f1d1103a64487cc74bb3e6c00543fc997af5547d`.
Runtime identities and the passed source CI are listed above; publication
follow-up does not change those measured checkpoints.
