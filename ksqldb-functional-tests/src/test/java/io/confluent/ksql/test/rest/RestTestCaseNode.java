/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test.rest;

import static io.confluent.ksql.test.utils.ImmutableCollections.immutableCopyOf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.test.model.RecordNode;
import io.confluent.ksql.test.model.TopicNode;
import io.confluent.ksql.test.rest.model.ExpectedErrorNode;
import io.confluent.ksql.test.rest.model.Response;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.tools.exceptions.MissingFieldException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JSON serializable Pojo representing the test case used by {@link RestQueryTranslationTest}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RestTestCaseNode {

  private final String name;
  private final List<String> formats;
  private final List<RecordNode> inputs;
  private final List<RecordNode> outputs;
  private final List<TopicNode> topics;
  private final List<String> statements;
  private final List<Response> responses;
  private final Map<String, Object> properties;
  private final Optional<ExpectedErrorNode> expectedError;
  private final Optional<InputConditions> inputConditions;
  private final boolean enabled;

  public RestTestCaseNode(
      @JsonProperty("name") final String name,
      @JsonProperty("format") final List<String> formats,
      @JsonProperty("inputs") final List<RecordNode> inputs,
      @JsonProperty("outputs") final List<RecordNode> outputs,
      @JsonProperty("topics") final List<TopicNode> topics,
      @JsonProperty("statements") final List<String> statements,
      @JsonProperty("properties") final Map<String, Object> properties,
      @JsonProperty("expectedError") final ExpectedErrorNode expectedError,
      @JsonProperty("responses") final List<Response> responses,
      @JsonProperty("inputConditions") final InputConditions inputConditions,
      @JsonProperty("enabled") final Boolean enabled
  ) {
    this.name = name == null ? "" : name;
    this.formats = immutableCopyOf(formats);
    this.statements = immutableCopyOf(statements);
    this.inputs = immutableCopyOf(inputs);
    this.outputs = immutableCopyOf(outputs);
    this.topics = immutableCopyOf(topics);
    this.properties = immutableCopyOf(properties);
    this.expectedError = Optional.ofNullable(expectedError);
    this.responses = immutableCopyOf(responses);
    this.inputConditions = Optional.ofNullable(inputConditions);
    this.enabled = !Boolean.FALSE.equals(enabled);

    validate();
  }

  public boolean isEnabled() {
    return enabled;
  }

  public String name() {
    return name;
  }

  public List<String> formats() {
    return formats;
  }

  public List<String> statements() {
    return statements;
  }

  public Optional<ExpectedErrorNode> expectedError() {
    return expectedError;
  }

  public List<TopicNode> topics() {
    return topics;
  }

  public List<RecordNode> inputs() {
    return inputs;
  }

  public List<RecordNode> outputs() {
    return outputs;
  }

  public List<Response> getResponses() {
    return responses;
  }

  public Map<String, Object> properties() {
    return properties;
  }

  public Optional<InputConditions> getInputConditions() {
    return inputConditions;
  }

  private void validate() {
    if (this.name.isEmpty()) {
      throw new MissingFieldException("name");
    }

    if (this.statements.isEmpty()) {
      throw new InvalidFieldException("statements", "was empty");
    }

    if (!this.inputs.isEmpty() && this.expectedError.isPresent()) {
      throw new InvalidFieldException("inputs and expectedError",
          "can not both be set");
    }
  }
}
