# KSQL JMH Microbenchmarks

This module is for JMH micro-benchmarking of pieces of the KSQL code.

## `SerdeBenchmark.java`

For example, `SerdeBenchmark.java`
benchmarks the performance of the serdes used by KSQL, since the serdes have been
shown to be a performance bottleneck in the past. The benchmarks use the schema files found in
`src/main/resources/schemas`. A serialization and deserialization benchmark is run for each 
configured combination of schema and format.  

### How to run

The benchmarks can be run either from `SerdeBenchmark.java` directly through IntelliJ, or via the
command line as follows, after building the module to produce `target/benchmarks.jar`:

```
java -jar ./target/benchmarks.jar
```

### Running a subset of benchmarks

To run only a subset of the benchmarks, you can specify parameters to run with. For example,
to run only Avro benchmarks:
```
java -jar ./target/benchmarks.jar -p params=impressions/Avro,metrics/Avro,single-key/Avro
```

Or to run only JSON (serialization and deserialization) benchmarks using the `metrics` schema:
```
java -jar ./target/benchmarks.jar -p params=metrics/JSON
```

### Running with non-default parameters

JMH parameters of interest may include the number of forks to use (`-f`), the number of warmup and
measurement iterations (`-wi` and `-i`, respectively), the duration of each iteration
(`-w` and `-r` for warmup and measurement iterations, respectively, with units of seconds),
and the number of threads (`-t`).
By default, `SerdeBenchmark.java` is set up to run with 3 forks, 3 warmup iterations, 3 measurement
iterations, 10 seconds per iteration, and 4 threads.

As an example, to run benchmarks with 8 threads and only a single fork:
```
java -jar ./target/benchmarks.jar -t 8 -f 1
```

The full list of JMH command line options can be viewed with:
```
java -jar ./target/benchmarks.jar -h
```

### Benchmark results

The following results were obtained from 10 runs of `SerdeBenchmark.java` on different
r5.xlarge EC2 instances, for KSQL code as of the 5.2 release (compiled and run with Java 11),
using the default benchmark parameters.

|  Benchmark  | serializationFormat | schemaName  | time per op (us) | standard deviation across 10 runs (us) |
|:-----------:|:-------------------:|:-----------:|:----------------:|:--------------------------------------:|
| deserialize |        JSON         | impressions |     2.794        |   0.059                                |
| deserialize |        Avro         | impressions |     4.708        |   0.162                                |
| deserialize |        JSON         |   metrics   |     16.077       |   0.501                                |
| deserialize |        Avro         |   metrics   |     16.993       |   0.401                                |
|  serialize  |        JSON         | impressions |     1.741        |   0.065                                |
|  serialize  |        Avro         | impressions |     2.774        |   0.097                                |
|  serialize  |        JSON         |   metrics   |     6.530        |   0.258                                |
|  serialize  |        Avro         |   metrics   |     9.904        |   0.300                                |

Time per operation is quite consistent from run to run, and from iteration to iteration within runs.
(The cross-instance variance was found to be greater than the run-to-run variance on a single
instance for many of the benchmarks.)
Don't be surprised if running on your laptop produces better results than those reported here for
an r5.xlarge EC2 instance, since that is consistently the case.

Results from 2019 Macbook Pro (2.3 Ghz cores), run with:

```java
@Warmup(iterations = 1, time = 30)
@Measurement(iterations = 2, time = 30)
@Threads(4)
@Fork(1)
```

| Benchmark   | schemaName & serialisationFormat | time (us/op) |
|:-----------:|:--------------------------------:|:------------:| 
| deserialize | single-key/Delimited  | 2.784 |
| deserialize |      single-key/Kafka  | 0.045 |
| deserialize |       single-key/JSON  | 0.200 |
| deserialize |       single-key/Avro  | 0.550 |
| deserialize | impressions/Delimited  | 2.840 |
| deserialize |  impressions/Protobuf  | 1.928 |
| deserialize |      impressions/JSON  | 1.218 |
| deserialize |      impressions/Avro  | 1.474 |
| deserialize |      metrics/Protobuf  | 5.259 |
| deserialize |          metrics/JSON  | 5.687 |
| deserialize |          metrics/Avro  | 4.674 |
| serialize   |  single-key/Delimited  | 0.200 |
| serialize   |      single-key/Kafka  | 0.010 |
| serialize   |       single-key/JSON  | 0.170 |
| serialize   |       single-key/Avro  | 0.245 |
| serialize   | impressions/Delimited  | 0.521 |
| serialize   |  impressions/Protobuf  | 1.471 |
| serialize   |      impressions/JSON  | 0.683 |
| serialize   |      impressions/Avro  | 1.374 |
| serialize   |      metrics/Protobuf  | 6.321 |
| serialize   |         metrics/JSON  | 3.336 |
| serialize   |          metrics/Avro  | 5.179 |