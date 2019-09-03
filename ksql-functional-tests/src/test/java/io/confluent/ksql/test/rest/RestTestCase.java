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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.test.rest.model.Response;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.Test;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.rest.entities.ErrorMessage;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hamcrest.Matcher;

@Immutable
class RestTestCase implements Test {

  private final Path testPath;
  private final String name;
  private final ImmutableList<Topic> topics;
  private final ImmutableList<Record> inputRecords;
  private final ImmutableList<Record> outputRecords;
  private final ImmutableList<String> statements;
  private final ImmutableList<Response> responses;
  private final Optional<Matcher<ErrorMessage>> expectedError;

  RestTestCase(
      final Path testPath,
      final String name,
      final Collection<Topic> topics,
      final Collection<Record> inputRecords,
      final Collection<Record> outputRecords,
      final Collection<String> statements,
      final Collection<Response> responses,
      final Optional<Matcher<ErrorMessage>> expectedError
  ) {
    this.name = requireNonNull(name, "name");
    this.testPath = requireNonNull(testPath, "testPath");
    this.topics = immutableCopyOf(requireNonNull(topics, "topics"));
    this.inputRecords = immutableCopyOf(requireNonNull(inputRecords, "inputRecords"));
    this.outputRecords = immutableCopyOf(requireNonNull(outputRecords, "outputRecords"));
    this.statements = immutableCopyOf(requireNonNull(statements, "statements"));
    this.responses = immutableCopyOf(requireNonNull(responses, "responses"));
    this.expectedError = requireNonNull(expectedError, "expectedError");
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getTestFile() {
    return testPath.toString();
  }

  List<Topic> getTopics() {
    return topics;
  }

  List<String> getStatements() {
    return statements;
  }

  Map<Topic, List<Record>> getInputsByTopic() {
    return inputRecords.stream()
        .collect(Collectors.groupingBy(Record::topic));
  }

  Map<Topic, List<Record>> getOutputsByTopic() {
    return outputRecords.stream()
        .collect(Collectors.groupingBy(Record::topic));
  }

  List<Response> getExpectedResponses() {
    return responses;
  }

  public Optional<Matcher<ErrorMessage>> expectedError() {
    return expectedError;
  }
}