package io.confluent.ksql.test.planned;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.test.model.TestCaseNode;
import java.util.Map;

@JsonInclude(Include.NON_ABSENT)
public class TestCaseSpecNode {

  private final String version;
  private final long timestamp;
  private final String path;
  private final Map<String, String> schemas;
  private final TestCaseNode testCase;

  public TestCaseSpecNode(
      @JsonProperty("version") final String version,
      @JsonProperty("timestamp") final long timestamp,
      @JsonProperty("path") final String path,
      @JsonProperty("schemas") final Map<String, String> schemas,
      @JsonProperty("testCase") final TestCaseNode testCase
  ) {
    this.version = requireNonNull(version, "version");
    this.timestamp = timestamp;
    this.path = requireNonNull(path, "path");
    this.schemas = requireNonNull(schemas, "schemas");
    this.testCase = requireNonNull(testCase, "testCase");
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getPath() {
    return path;
  }

  public Map<String, String> getSchemas() {
    return schemas;
  }

  public String getVersion() {
    return version;
  }

  public TestCaseNode getTestCase() {
    return testCase;
  }
}
