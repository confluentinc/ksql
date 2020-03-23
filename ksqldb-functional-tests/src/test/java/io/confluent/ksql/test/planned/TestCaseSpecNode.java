package io.confluent.ksql.test.planned;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.test.model.PostConditionsNode;
import io.confluent.ksql.test.model.RecordNode;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonInclude(Include.NON_ABSENT)
public class TestCaseSpecNode {

  private final String version;
  private final long timestamp;
  private final Map<String, String> schemas;
  private final List<RecordNode> inputs;
  private final List<RecordNode> outputs;
  private final Optional<PostConditionsNode> postConditions;

  public TestCaseSpecNode(
      @JsonProperty("version") final String version,
      @JsonProperty("timestamp") final long timestamp,
      @JsonProperty("schemas") final Map<String, String> schemas,
      @JsonProperty("inputs") final List<RecordNode> inputs,
      @JsonProperty("outputs") final List<RecordNode> outputs,
      @JsonProperty("post") final Optional<PostConditionsNode> postConditions
  ) {
    this.version = Objects.requireNonNull(version, "version");
    this.timestamp = timestamp;
    this.schemas = Objects.requireNonNull(schemas, "schemas");
    this.inputs = ImmutableList.copyOf(Objects.requireNonNull(inputs, "inputs"));
    this.outputs = ImmutableList.copyOf(Objects.requireNonNull(outputs, "outputs"));
    this.postConditions = Objects.requireNonNull(postConditions, "postConditions");
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Map<String, String> getSchemas() {
    return schemas;
  }

  public String getVersion() {
    return version;
  }

  public List<RecordNode> getInputs() {
    return inputs;
  }

  public List<RecordNode> getOutputs() {
    return outputs;
  }

  public Optional<PostConditionsNode> getPostConditions() {
    return postConditions;
  }
}
