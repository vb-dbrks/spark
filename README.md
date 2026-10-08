# SPARK-60085: Float/Double map lookup evidence

Baseline: Apache Spark [`0c14861e323174452d42ffb7947d1c1158c39286`](https://github.com/apache/spark/tree/0c14861e323174452d42ffb7947d1c1158c39286).
The measured runtime change is [production.patch](production.patch), also included in
[the PR source commit](https://github.com/vb-dbrks/spark/commit/c6be3c197c7ef4d3d760593e29f7be16a9602534).
Float bits use Spark's existing
`Murmur3_x86_32.hashInt` and full Double bits use `hashLong`, seed 42, with both zero signs
normalized before hashing. JDK bit conversion canonicalizes NaNs. Table construction and
generated lookup use matching formulas. Other types and strategy selection are unchanged.

The change substantially helps clustered floating keys and these TargetEncoder workloads.
It has a measured tradeoff: random-map missing-key cases become slower. This package includes
all cases and all samples from the complete TargetEncoder and final Range experiments,
including regressions and unchanged Integer controls. It does not claim universal speedups.

## Environment and protocol

Apple M3 Pro, 36 GiB RAM, macOS 26.6.2; Temurin JDK 21.0.5+11-LTS arm64;
Spark's bundled SBT 1.12.13; each benchmark JVM has a 4 GiB heap. Both variants use identical
compiled dependencies and harness classes, swapping only the saved Catalyst jar.
Artifact/source hashes are in [provenance.json](provenance.json); jars are deliberately omitted.

Both experiments use `local[1]`, one input/shuffle partition, AQE disabled, and seed 52878.
The map threshold (1,000), whole-stage codegen (enabled), and codegen factory (`FALLBACK`)
retain their defaults. Three fresh JVMs per variant, five measured iterations per case.
Round 2 reverses both variant order and case order. Configuration is set before queries.

- **TargetEncoder:** 100,000 transform rows; at least two seconds of warmup. Actual fitted
  models with 1,000/10,000 categories, continuous target `category % 7`, five training rows
  per category, smoothing zero. Fitting and input preparation are outside timing. Transform
  input is cached Spark Range. Both repeated prepared-query and fresh-query execution are
  measured; encoded output sums are independently checked. Four cases, 120 raw samples.
- **Range controls:** 1,000,000 lookup rows; at least five seconds of warmup. Dense 1,000-key
  maps and random/sparse 10,000-key maps. Runtime keys come from a cached Range projection
  into a typed literal array. The map is foldable, the lookup key is not; analyzed types,
  generated hash strategy, loaded Catalyst origin and cache scans are checked. Inputs and
  diagnostics are prepared before timing. Values are ordinal Longs; sums are independently
  calculated. Missing keys are negative integers -1 through -10,000 converted to the actual
  key type and checked absent. They are **not random fractional missing keys**. Eleven cases,
  330 raw samples.

Times include the Spark action, aggregate collection and checksum assertion. CSV writes are
outside the measured interval. Milliseconds below are medians of three per-JVM medians.
Speedup is baseline/candidate; runtime change is candidate/baseline minus one. Ranges compare
corresponding round medians and are descriptive, not confidence intervals. Positive runtime
change means slower candidate execution. Absolute times between the two experiments are
not comparable: their row counts and queries differ.

## All retained results

### target-encoder

| Case | Baseline ms | Candidate ms | Paired speedup | Paired runtime change |
|---|---:|---:|---:|---:|
| target-encoder-1000-default-fresh | 148.44 | 29.79 | 3.92-5.44x | -81.6% to -74.5% |
| target-encoder-1000-default-repeat | 127.32 | 12.01 | 9.69-13.29x | -92.5% to -89.7% |
| target-encoder-10000-default-fresh | 86.85 | 59.90 | 1.40-1.62x | -38.1% to -28.3% |
| target-encoder-10000-default-repeat | 52.26 | 27.24 | 1.83-2.07x | -51.6% to -45.3% |

### range

| Case | Baseline ms | Candidate ms | Paired speedup | Paired runtime change |
|---|---:|---:|---:|---:|
| double-dense-1000 | 617.50 | 23.61 | 24.47-27.41x | -96.4% to -95.9% |
| double-random-10000 | 35.12 | 37.28 | 0.91-0.97x | +2.8% to +9.7% |
| double-random-10000-miss | 29.62 | 38.75 | 0.76-0.80x | +25.2% to +30.8% |
| double-sparse-10000 | 37.72 | 43.26 | 0.87-1.13x | -11.9% to +14.7% |
| float-dense-1000 | 789.38 | 24.81 | 23.05-38.25x | -97.4% to -95.7% |
| float-random-10000 | 37.85 | 37.01 | 0.93-1.18x | -15.4% to +7.6% |
| float-random-10000-miss | 31.60 | 36.06 | 0.77-0.91x | +9.3% to +30.3% |
| float-sparse-10000 | 81.05 | 39.15 | 2.00-2.24x | -55.4% to -50.1% |
| integer-dense-10000 | 28.39 | 26.50 | 0.95-1.29x | -22.4% to +5.5% |
| integer-random-10000 | 32.09 | 38.62 | 0.81-1.13x | -11.7% to +23.8% |
| integer-random-10000-miss | 32.31 | 31.43 | 0.86-1.09x | -8.1% to +16.2% |

All 450 samples retained; 3 exceed twice their five-sample group median.

## Recompute the table

Run `python3 analyze.py` from this directory. It reads the 12 included raw CSV files, checks
five iterations per case and 450 total samples, and prints every row above. The raw CSV schema
is `case,iteration,elapsed_nanoseconds` without a header. No sample is discarded. The outlier
flag means greater than twice that case/JVM group's median; it does not diagnose a pause.
[SHA256SUMS](SHA256SUMS) records the package contents.

## Reproduce the Spark runs

The custom harness is necessary; these entry points are not present in unmodified upstream
Spark. [harness/benchmarks.patch](harness/benchmarks.patch) contains the complete historical
SQL/TargetEncoder helper additions; this reproduction executes its TargetEncoder entry point
only. The final SQL controls use [RangeMapLookupControls.scala](harness/RangeMapLookupControls.scala)
instead of the helper's earlier RDD-backed SQL input. These evidence files are separate from
the production PR diff.

Use Linux/macOS, JDK 21, Python 3.9+ and an isolated new Spark checkout. The commands use public default
dependency repositories; no private infrastructure or credentials are needed. Expect a full
Spark source build. Starting in this evidence directory:

```sh
export MAP_EVIDENCE="$PWD"
export MAP_OUTPUT="$PWD/../fresh-map-evidence-run"
# Set JAVA_HOME to your installed JDK 21 before building.
git clone https://github.com/apache/spark.git ../spark-map-reproduction
cd ../spark-map-reproduction
git checkout --detach 0c14861e323174452d42ffb7947d1c1158c39286
git apply "$MAP_EVIDENCE/harness/benchmarks.patch"
cp "$MAP_EVIDENCE/harness/RangeMapLookupControls.scala" sql/core/src/test/scala/org/apache/spark/sql/execution/benchmark/
./build/sbt "sql/Test/compile" "mllib/Test/compile" "show sql/Test/fullClasspath" "show mllib/Test/fullClasspath"
python3 "$MAP_EVIDENCE/reproduce.py" snapshot-baseline --spark "$PWD" --output "$MAP_OUTPUT"
git apply "$MAP_EVIDENCE/production.patch"
./build/sbt "sql/Test/compile" "mllib/Test/compile" "show sql/Test/fullClasspath" "show mllib/Test/fullClasspath"
python3 "$MAP_EVIDENCE/reproduce.py" snapshot-candidate --spark "$PWD" --output "$MAP_OUTPUT"
python3 "$MAP_EVIDENCE/reproduce.py" rounds --spark "$PWD" --output "$MAP_OUTPUT"
```

The driver runs one process at a time, records commands/logs/raw samples, verifies Catalyst
origin and generated hash code, and refuses to overwrite results. It takes JVM flags from
[java-options.json](java-options.json), adding only local output/check-out paths. Its CLI uses
portable supplied paths rather than the original author's paths. **This public wrapper is
an adaptation and has been syntax-checked, not rerun end to end.** The Scala harness, jar
comparison method and supplied samples are the actual locally executed experiments. Rebuilt
jars may have different binary hashes; the snapshot step checks the exact production source
hashes before accepting each variant. Run with other benchmarks/builds idle.

## Limits and scope

These are constructed workloads on one machine. The same seed and round pairing reduce
some variation, but three pairs do not establish a population estimate. Unchanged Integer
controls drift too, so small timing differences are weaker evidence than the large clustered
key gains. The consistent Float/Double miss regressions must accompany the improvement claim.
Hash arithmetic and changed probe patterns are not separated here, and construction cost is
not isolated. TargetEncoder still performs its existing repeated lookups.

Earlier SQL measurements used a cached collection-backed input whose partition payload could
still be serialized with tasks. They are preserved locally as historical evidence, but
**are not used in these public tables**. The final Range controls avoid that particular input
confound. This package is a benchmark evidence subset, not a claim that every earlier
experiment or test log is included. The interpreted Java-index signed-zero behavior is outside
this production change. No cloud-workspace validation or CI success is claimed.

## License

The code in this evidence package is provided under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
