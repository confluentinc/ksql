# Deprecation Tracker for ksqlDB

This tool identifies and tracks **unique** deprecated methods and configurations from **external dependencies** that ksqlDB uses.

## What It Tracks

The tracker focuses on deprecated APIs from external libraries:

- **Apache Kafka** (clients, streams)
- **Vert.x** (HTTP server, web)
- **Jackson** (JSON processing)
- **Apache Commons** (lang3, text)
- **Jetty** (servlets)
- **Log4j2**
- Other dependencies

**Note:** This does NOT track `@Deprecated` annotations within ksqlDB's own source code. It only tracks deprecated APIs from libraries that ksqlDB depends on.

## Quick Start

### Run Full Deprecation Analysis

From the project root:

```bash
./scripts/deprecation-tracker/extract-deprecations.sh
```

This will:
1. Run a Maven compile
2. Extract all **unique** deprecated API usages from external libraries
3. Identify **unique** deprecated configuration patterns
4. Generate reports in `target/deprecation-report/`

### Analyze Existing Build Log

If you already have a Maven build log:

```bash
./scripts/deprecation-tracker/extract-deprecations.sh path/to/build.log
```

## Maven Profiles

Two Maven profiles are available for deprecation checking:

### Standard Deprecation Check

Shows all deprecation warnings during compile:

```bash
mvn clean compile -P deprecation-check
```

### Strict Deprecation Check

Fails the build if any deprecation warnings are found:

```bash
mvn clean compile -P deprecation-strict
```

## Output Reports

Reports are generated in `target/deprecation-report/`:

| File | Description |
|------|-------------|
| `unique-deprecated-apis-latest.txt` | Unique deprecated APIs from dependencies |
| `unique-deprecated-apis-latest.json` | JSON format for programmatic processing |
| `unique-deprecated-configs-latest.txt` | Unique deprecated config patterns |
| `unique-deprecated-configs-latest.json` | JSON format for configs |
| `deprecation-summary-latest.txt` | Summary with counts |
| `deprecated-api-details-latest.txt` | Full details of all deprecation warnings |

## CI Integration

The Semaphore CI pipeline runs deprecation analysis as part of the Test job's epilogue:

1. After maven build completes, the deprecation tracker analyzes the build log
2. Generates reports in `target/deprecation-report/`
3. Uploads reports as a workflow artifact

### Viewing Reports in CI

After a CI build:
1. Download the `target/deprecation-report` artifact from the workflow
2. View the reports:
   - `deprecation-summary-latest.txt` - Summary with counts
   - `unique-deprecated-apis-latest.txt` - List of unique deprecated APIs
   - `unique-deprecated-configs-latest.txt` - List of unique deprecated configs
   - `*.json` files - For programmatic processing

## Understanding the Reports

### Unique Deprecated APIs Report

Shows each deprecated API from external dependencies **once**, even if used multiple times:

```
  1. ProductionExceptionHandlerResponse in org.apache.kafka.streams.errors.ProductionExceptionHandler
     Occurrences: 39

  2. KafkaStreamsNamedTopologyWrapper in org.apache.kafka.streams.processor.internals.namedtopology
     Occurrences: 38

  3. io.vertx.core.logging.Logger in io.vertx.core.logging
     Occurrences: 15
```

### Unique Deprecated Configs Report

Lists deprecated configuration keys from external libraries:

```
  1. CACHE_MAX_BYTES_BUFFERING_CONFIG
     Library: Kafka Streams
     Reason: Use statestore.cache.max.bytes instead
     Found in:
       ksqldb-common/src/main/java/io/confluent/ksql/util/KsqlConfig.java:1704

  2. DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG
     Library: Kafka Streams
     Reason: Use deserialization.exception.handler instead
```

## Common Deprecated APIs by Library

### Apache Kafka
- `ProductionExceptionHandlerResponse` - Use new exception handling API
- `DeserializationHandlerResponse` - Use new exception handling API
- `Topology.AutoOffsetReset` - Use new offset reset configuration
- `KafkaStreamsNamedTopologyWrapper` - Internal API changes
- `ConsumerRecords` constructor - Use builder pattern

### Vert.x
- `io.vertx.core.logging.Logger` - Use SLF4J instead
- `io.vertx.core.logging.LoggerFactory` - Use SLF4J instead
- `host()` in HttpServerRequest - Use authority() instead
- `getBody()` in RoutingContext - Use body() instead

### Jackson
- `withExactBigDecimals()` - Configuration change
- `fields()` in ObjectNode - Use properties() instead

### Apache Commons
- `ObjectUtils.defaultIfNull()` - Use Objects.requireNonNullElse()
- `org.apache.commons.lang3.text.WordUtils` - Use new text package

## JSON Output for Automation

The JSON reports can be used for automated tracking:

```json
{
  "generated": "2024-01-15T10:30:00",
  "description": "Deprecated APIs from external dependencies used by ksqlDB",
  "compiler_warnings": {
    "total_warnings": 388,
    "unique_api_count": 65
  },
  "unique_deprecated_apis": [
    {"api": "ProductionExceptionHandlerResponse in ...", "occurrences": 39, "type": "compiler"},
    {"api": "io.vertx.core.logging.Logger in ...", "occurrences": 15, "type": "compiler"}
  ]
}
```

Use `jq` to extract specific information:

```bash
# Count of unique deprecated APIs
jq '.compiler_warnings.unique_api_count' target/deprecation-report/unique-deprecated-apis-latest.json

# List just the API names
jq -r '.unique_deprecated_apis[].api' target/deprecation-report/unique-deprecated-apis-latest.json

# Get APIs with more than 10 occurrences
jq '.unique_deprecated_apis[] | select(.occurrences > 10)' target/deprecation-report/unique-deprecated-apis-latest.json
```

## Reducing Deprecations

When addressing deprecations from dependencies:

1. **Review the unique report** - Focus on the unique list, not individual occurrences
2. **Prioritize by occurrence count** - High-count deprecations are used more widely
3. **Check library documentation** - Look for migration guides
4. **Consider library upgrades** - Sometimes newer versions remove the deprecated APIs
5. **Create tracking issues** - File JIRA tickets for each unique deprecation
6. **Update incrementally** - Fix deprecations as part of regular development

## Troubleshooting

### No deprecations found

Ensure the Maven build completes successfully. The script parses build output for warnings.

### Script fails to parse

The script expects standard Maven/Java compiler output. If using a custom build wrapper, ensure deprecation warnings are not filtered.

### Add more deprecated config patterns

Edit the `check_config` calls in `extract-deprecations.sh` to add more patterns to check.
