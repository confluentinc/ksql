# KSQL JMH Microbenchmarks

This module is for JMH micro-benchmarking of pieces of the KSQL code.

## `SerdeBenchmark.java`

For example, `SerdeBenchmark.java`
benchmarks the performance of the Avro and JSON serdes used by KSQL, since the serdes have been
shown to be a performance bottleneck in the past. The benchmarks use the schema files found in
`src/main/resources/schemas`. A serialization and deserialization benchmark is run for each schema
(e.g., `impressions` or `metrics`) and each serialization format (Avro or JSON).  

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
java -jar ./target/benchmarks.jar -p serializationFormat=Avro
```

Or to run only JSON (serialization and deserialization) benchmarks using the `metrics` schema:
```
java -jar ./target/benchmarks.jar -p serializationFormat=JSON -p schemaName=metrics
```

Or to run only the deserialization benchmarks on both the `impressions` and `metrics` schemas:
```
java -jar ./target/benchmarks.jar SerdeBenchmark.deserialize -p schemaName=impressions,metrics
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

The following results were obtained from 20 runs of `SerdeBenchmark.java` on an
r5.xlarge EC2 instance, for KSQL code as of the 5.2 release (compiled and run with Java 11),
using the default benchmark parameters.

|  Benchmark  | serializationFormat | schemaName  | time per op (us) | standard deviation across 20 runs (us) |
|:-----------:|:-------------------:|:-----------:|:----------------:|:--------------------------------------:|
| deserialize |        JSON         | impressions |     2.840        |   0.070                                |
| deserialize |        Avro         | impressions |     4.748        |   0.062                                |
| deserialize |        JSON         |   metrics   |     16.309       |   0.162                                |
| deserialize |        Avro         |   metrics   |     17.423       |   1.045                                |
|  serialize  |        JSON         | impressions |     1.782        |   0.071                                |
|  serialize  |        Avro         | impressions |     2.848        |   0.172                                |
|  serialize  |        JSON         |   metrics   |     6.620        |   0.149                                |
|  serialize  |        Avro         |   metrics   |     10.056       |   0.099                                |

Time per operation is quite consistent from run to run, and from iteration to iteration within runs.
Don't be surprised if running on your laptop produces better results than those reported here for
an r5.xlarge EC2 instance, since that is consistently the case.