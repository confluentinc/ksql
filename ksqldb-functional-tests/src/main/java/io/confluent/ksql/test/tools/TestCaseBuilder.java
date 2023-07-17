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

package io.confluent.ksql.test.tools;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.test.model.ExpectedExceptionNode;
import io.confluent.ksql.test.model.PostConditionsNode;
import io.confluent.ksql.test.model.RecordNode;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.model.TestLocation;
import io.confluent.ksql.test.model.TopicNode;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.Matcher;

/**
 * Builds {@link TestCase}s from {@link TestCaseNode}.
 */
public final class TestCaseBuilder {

  private TestCaseBuilder() {
  }

  public static List<TestCase> buildTests(
      final TestCaseNode test,
      final Path originalFileName,
      final Function<String, TestLocation> testLocator
  ) {
    if (!test.isEnabled()) {
      return ImmutableList.of();
    }

    try {
      final Stream<Optional<String>> formats = test.formats().isEmpty()
          ? Stream.of(Optional.empty())
          : test.formats().stream().map(Optional::of);

      final TestLocation location = testLocator.apply(test.name());

      return formats
          .map(format -> createTest(test, originalFileName, location, format))
          .collect(Collectors.toList());
    } catch (final Exception e) {
      throw new AssertionError("Invalid test '" + test.name() + "': " + e.getMessage(), e);
    }
  }

  private static TestCase createTest(
      final TestCaseNode test,
      final Path originalFileName,
      final TestLocation location,
      final Optional<String> explicitFormat
  ) {
    final String testName = TestCaseBuilderUtil.buildTestName(
        originalFileName,
        test.name(),
        explicitFormat
    );

    try {
      final VersionBounds versionBounds = test.versionBounds().build();

      final List<String> statements = TestCaseBuilderUtil.buildStatements(
          test.statements(),
          explicitFormat
      );

      final Optional<Matcher<Throwable>> ee = test.expectedException()
          .map(ExpectedExceptionNode::build);

      final List<Topic> topics = test.topics().stream()
          .map(TopicNode::build)
          .collect(Collectors.toList());

      final List<Record> inputRecords = test.inputs().stream()
          .map(RecordNode::build)
          .collect(Collectors.toList());

      final List<Record> outputRecords = test.outputs().stream()
          .map(RecordNode::build)
          .collect(Collectors.toList());

      final PostConditions post = test.postConditions()
          .map(PostConditionsNode::build)
          .orElse(PostConditions.NONE);

      return new TestCase(
          location,
          originalFileName,
          testName,
          versionBounds,
          test.properties(),
          topics,
          inputRecords,
          outputRecords,
          statements,
          ee,
          post
      );
    } catch (final Exception e) {
      throw new AssertionError(testName + ": Invalid test. " + e.getMessage(), e);
    }
  }
}
