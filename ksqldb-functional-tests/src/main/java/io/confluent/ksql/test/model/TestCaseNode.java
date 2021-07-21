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


package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.tools.exceptions.MissingFieldException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JSON serializable Pojo representing a test case.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TestCaseNode {

  private final String name;
  private final VersionBoundsNode versionBounds;
  private final List<String> formats;
  private final List<RecordNode> inputs;
  private final List<RecordNode> outputs;
  private final List<TopicNode> topics;
  private final List<String> statements;
  private final Map<String, Object> properties;
  private final Optional<ExpectedExceptionNode> expectedException;
  private final Optional<PostConditionsNode> postConditions;
  private final boolean enabled;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public TestCaseNode(
      @JsonProperty("name") final String name,
      @JsonProperty("versions") final Optional<VersionBoundsNode> versionBounds,
      @JsonProperty("format") final List<String> formats,
      @JsonProperty("inputs") final List<RecordNode> inputs,
      @JsonProperty("outputs") final List<RecordNode> outputs,
      @JsonProperty("topics") final List<TopicNode> topics,
      @JsonProperty("statements") final List<String> statements,
      @JsonProperty("properties") final Map<String, Object> properties,
      @JsonProperty("expectedException") final ExpectedExceptionNode expectedException,
      @JsonProperty("post") final PostConditionsNode postConditions,
//      @JsonProperty("conditionalInputs") final ConditionalInputs conditionalInputs,
      @JsonProperty("enabled") final Boolean enabled
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.name = name == null ? "" : name;
    this.formats = immutableCopyOf(formats);
    this.versionBounds = requireNonNull(versionBounds).orElse(VersionBoundsNode.allVersions());
    this.statements = immutableCopyOf(statements);
    this.inputs = immutableCopyOf(inputs);
    this.outputs = immutableCopyOf(outputs);
    this.topics = immutableCopyOf(topics);
    this.properties = immutableCopyOf(properties);
    this.expectedException = Optional.ofNullable(expectedException);
    this.postConditions = Optional.ofNullable(postConditions);
    this.enabled = !Boolean.FALSE.equals(enabled);

    validate();
  }

  @JsonIgnore
  public boolean isEnabled() {
    return enabled;
  }

  @JsonProperty("name")
  public String name() {
    return name;
  }

  @JsonIgnore
  public VersionBoundsNode versionBounds() {
    return versionBounds;
  }

  @JsonIgnore
  public List<String> formats() {
    return formats;
  }

  @JsonProperty("statements")
  public List<String> statements() {
    return statements;
  }

  @JsonIgnore
  public Optional<ExpectedExceptionNode> expectedException() {
    return expectedException;
  }

  @JsonProperty("topics")
  public List<TopicNode> topics() {
    return topics;
  }

  @JsonProperty("inputs")
  public List<RecordNode> inputs() {
    return inputs;
  }

  @JsonProperty("outputs")
  public List<RecordNode> outputs() {
    return outputs;
  }

  @JsonProperty("post")
  public Optional<PostConditionsNode> postConditions() {
    return postConditions;
  }

  @JsonProperty("properties")
  public Map<String, Object> properties() {
    return properties;
  }

  private void validate() {
    if (name.isEmpty()) {
      throw new MissingFieldException("name");
    }

    try {
      if (statements.isEmpty()) {
        throw new InvalidFieldException("statements", "was empty");
      }

      if (!expectedException.isPresent() && inputs.isEmpty() && outputs.isEmpty()) {
        throw new InvalidFieldException(
            "outputs", "no inputs, outputs or expectedException provided");
      }
    } catch (final Exception e) {
      throw new TestFrameworkException(
          "Invalid test case: '" + name + "'. cause: " + e.getMessage(), e);
    }
  }

  private static <T> ImmutableList<T> immutableCopyOf(final List<T> source) {
    return source == null ? ImmutableList.of() : ImmutableList.copyOf(source);
  }

  private static <K, V> ImmutableMap<K, V> immutableCopyOf(final Map<K, V> source) {
    return source == null ? ImmutableMap.of() : ImmutableMap.copyOf(source);
  }
}
