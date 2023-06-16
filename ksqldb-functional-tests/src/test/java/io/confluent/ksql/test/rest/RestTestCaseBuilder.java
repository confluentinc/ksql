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

import com.google.common.collect.Iterables;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.test.model.RecordNode;
import io.confluent.ksql.test.model.TestFileContext;
import io.confluent.ksql.test.model.TestLocation;
import io.confluent.ksql.test.model.TopicNode;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.TestCaseBuilderUtil;
import io.confluent.ksql.test.tools.Topic;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.Matcher;

/**
 * Builds {@link RestTestCase}s from {@link RestTestCaseNode}.
 */
final class RestTestCaseBuilder {

  private RestTestCaseBuilder() {
  }

  static Stream<RestTestCase> buildTests(
      final RestTestCaseNode test,
      final TestFileContext ctx
  ) {
    if (!test.isEnabled()) {
      return Stream.of();
    }

    try {
      final Stream<Optional<String>> formats = test.formats().isEmpty()
          ? Stream.of(Optional.empty())
          : test.formats().stream().map(Optional::of);

      final TestLocation location = ctx.getTestLocation(test.name());

      return formats
          .map(format -> createTest(test, format, Optional.empty(), location));
    } catch (final Exception e) {
      throw new AssertionError("Invalid test '" + test.name() + "': " + e.getMessage(), e);
    }
  }

  private static RestTestCase createTest(
      final RestTestCaseNode test,
      final Optional<String> explicitFormat,
      final Optional<String> config,
      final TestLocation location
  ) {
    final String testName = TestCaseBuilderUtil.buildTestName(
        location.getTestPath(),
        test.name(),
        explicitFormat,
        config
    );

    try {
      final List<String> statements = TestCaseBuilderUtil.buildStatements(
          test.statements(),
          explicitFormat
      );

      final Optional<Matcher<RestResponse<?>>> ee = test.expectedError()
          .map(een -> een.build(Iterables.getLast(statements)));

      final List<Topic> topics = test.topics().stream()
          .map(TopicNode::build)
          .collect(Collectors.toList());

      final List<Record> inputRecords = test.inputs().stream()
          .map(RecordNode::build)
          .collect(Collectors.toList());

      final List<Record> outputRecords = test.outputs().stream()
          .map(RecordNode::build)
          .collect(Collectors.toList());

      return new RestTestCase(
          location,
          testName,
          test.properties(),
          topics,
          inputRecords,
          outputRecords,
          statements,
          test.getResponses(),
          ee,
          test.getInputConditions(),
          test.getOutputConditions()
      );
    } catch (final Exception e) {
      throw new AssertionError(testName + ": Invalid test. " + e.getMessage(), e);
    }
  }
}



