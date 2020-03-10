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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.TestCaseBuilderUtil;
import io.confluent.ksql.test.tools.Topic;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.Matcher;

/**
 * Builds {@link RestTestCase}s from {@link RestTestCaseNode}.
 */
final class RestTestCaseBuilder {

  private final FunctionRegistry functionRegistry = TestFunctionRegistry.INSTANCE.get();

  List<RestTestCase> buildTests(final RestTestCaseNode test, final Path testPath) {
    if (!test.isEnabled()) {
      return ImmutableList.of();
    }

    try {
      final Stream<Optional<String>> formats = test.formats().isEmpty()
          ? Stream.of(Optional.empty())
          : test.formats().stream().map(Optional::of);

      return formats
          .map(format -> createTest(test, format, testPath))
          .collect(Collectors.toList());
    } catch (final Exception e) {
      throw new AssertionError("Invalid test '" + test.name() + "': " + e.getMessage(), e);
    }
  }

  private RestTestCase createTest(
      final RestTestCaseNode test,
      final Optional<String> explicitFormat,
      final Path testPath
  ) {
    final String testName = TestCaseBuilderUtil.buildTestName(
        testPath,
        test.name(),
        explicitFormat
    );

    try {
      final List<String> statements = TestCaseBuilderUtil.buildStatements(
          test.statements(),
          explicitFormat
      );

      final Optional<Matcher<RestResponse<?>>> ee = test.expectedError()
          .map(een -> een.build(Iterables.getLast(statements)));

      final Map<String, Topic> topics = TestCaseBuilderUtil.getTopicsByName(
          statements,
          test.topics(),
          test.outputs(),
          test.inputs(),
          ee.isPresent(),
          functionRegistry
      );

      final List<Record> inputRecords = test.inputs().stream()
          .map(node -> node.build(topics))
          .collect(Collectors.toList());

      final List<Record> outputRecords = test.outputs().stream()
          .map(node -> node.build(topics))
          .collect(Collectors.toList());

      return new RestTestCase(
          testPath,
          testName,
          topics.values(),
          inputRecords,
          outputRecords,
          statements,
          test.getResponses(),
          ee
      );
    } catch (final Exception e) {
      throw new AssertionError(testName + ": Invalid test. " + e.getMessage(), e);
    }
  }
}



