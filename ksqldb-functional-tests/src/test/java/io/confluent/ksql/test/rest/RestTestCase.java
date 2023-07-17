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
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.test.model.TestLocation;
import io.confluent.ksql.test.rest.model.Response;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.Test;
import io.confluent.ksql.test.tools.Topic;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hamcrest.Matcher;

class RestTestCase implements Test {

  private final TestLocation location;
  private final String name;
  private final Map<String, Object> properties;
  private final ImmutableList<Topic> topics;
  private final ImmutableList<Record> inputRecords;
  private final ImmutableList<Record> outputRecords;
  private final ImmutableList<String> statements;
  private final ImmutableList<Response> responses;
  private final Optional<Matcher<RestResponse<?>>> expectedError;
  private final Optional<InputConditions> inputConditions;

  RestTestCase(
      final TestLocation location,
      final String name,
      final Map<String, Object> properties,
      final Collection<Topic> topics,
      final Collection<Record> inputRecords,
      final Collection<Record> outputRecords,
      final Collection<String> statements,
      final Collection<Response> responses,
      final Optional<Matcher<RestResponse<?>>> expectedError,
      final Optional<InputConditions> inputConditions
  ) {
    this.name = requireNonNull(name, "name");
    this.location = requireNonNull(location, "testPath");
    this.properties = immutableCopyOf(requireNonNull(properties, "properties"));
    this.topics = immutableCopyOf(requireNonNull(topics, "topics"));
    this.inputRecords = immutableCopyOf(requireNonNull(inputRecords, "inputRecords"));
    this.outputRecords = immutableCopyOf(requireNonNull(outputRecords, "outputRecords"));
    this.statements = immutableCopyOf(requireNonNull(statements, "statements"));
    this.responses = immutableCopyOf(requireNonNull(responses, "responses"));
    this.expectedError = requireNonNull(expectedError, "expectedError");
    this.inputConditions = requireNonNull(inputConditions, "inputConditions");
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public TestLocation getTestLocation() {
    return location;
  }

  List<Topic> getTopics() {
    return topics;
  }

  List<String> getStatements() {
    return statements;
  }

  public ImmutableList<Record> getInputRecords() {
    return inputRecords;
  }

  public ImmutableList<Record> getOutputRecords() {
    return outputRecords;
  }

  Map<String, List<Record>> getInputsByTopic() {
    return inputRecords.stream()
        .collect(Collectors.groupingBy(Record::getTopicName));
  }

  Map<String, List<Record>> getOutputsByTopic() {
    return outputRecords.stream()
        .collect(Collectors.groupingBy(Record::getTopicName));
  }

  List<Response> getExpectedResponses() {
    return responses;
  }

  public Optional<Matcher<RestResponse<?>>> expectedError() {
    return expectedError;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public Optional<InputConditions> getInputConditions() {
    return inputConditions;
  }
}