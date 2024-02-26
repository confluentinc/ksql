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
import com.google.common.collect.Iterables;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.test.model.ExpectedExceptionNode;
import io.confluent.ksql.test.model.PostConditionsNode;
import io.confluent.ksql.test.model.RecordNode;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.model.TestLocation;
import io.confluent.ksql.test.model.TopicNode;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.util.KsqlConfig;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
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
      final List<TestCase> testCases = new LinkedList<>();

      final List<Optional<String>> formats = test.formats().isEmpty()
          ? Collections.singletonList(Optional.empty())
          : test.formats().stream().map(Optional::of).collect(Collectors.toList());

      final List<Optional<String>> configs;
      final Entry<String, Object> overwrite;

      if (test.config().isEmpty()) {
        configs = Collections.singletonList(Optional.empty());
        overwrite = null;
      } else {
        configs = test.config().stream().map(Optional::of).collect(Collectors.toList());

        overwrite = Iterables.getOnlyElement(
            test.properties().entrySet().stream()
                .filter(e -> {
                  final Object v = e.getValue();
                  return v instanceof String && "{CONFIG}".equalsIgnoreCase((String) v);
                })
                .collect(Collectors.toList()));
      }

      for (final Optional<String> format : formats) {
        for (final Optional<String> config : configs) {
          final TestLocation location = testLocator.apply(test.name());

          final Map<String, Object> updatedProperties = new HashMap<>(test.properties());
          final Optional<String> cfg;
          if (config.isPresent()) {
            updatedProperties.put(overwrite.getKey(), config.get());
            cfg = Optional.of(overwrite.getKey() + "=" + config.get());
          } else {
            cfg = Optional.empty();
          }

          testCases.add(createTest(
              new TestCaseNode(test, updatedProperties),
              originalFileName,
              location,
              format,
              cfg
          ));
        }
      }

      return testCases;
    } catch (final Exception e) {
      throw new AssertionError("Invalid test '" + test.name() + "': " + e.getMessage(), e);
    }
  }

  private static TestCase createTest(
      final TestCaseNode test,
      final Path originalFileName,
      final TestLocation location,
      final Optional<String> explicitFormat,
      final Optional<String> config
  ) {
    final String testName = TestCaseBuilderUtil.buildTestName(
        originalFileName,
        test.name(),
        explicitFormat,
        config
    );

    try {
      final VersionBounds versionBounds = test.versionBounds().build();

      final List<String> statements = TestCaseBuilderUtil.buildStatements(
          test.statements(),
          explicitFormat
      );

      final Optional<Matcher<Throwable>> ee = test.expectedException()
          .map(ExpectedExceptionNode::build);

      final Map<String, Topic> topics = TestCaseBuilderUtil
          .getAllTopics(
              statements,
              test.topics().stream().map(TopicNode::build).collect(Collectors.toList()),
              test.outputs().stream().map(RecordNode::build).collect(Collectors.toList()),
              test.inputs().stream().map(RecordNode::build).collect(Collectors.toList()),
              new InternalFunctionRegistry(),
              new KsqlConfig(test.properties())
          )
          .stream()
          .collect(Collectors.toMap(
              t -> t.getName().toLowerCase(),
              t -> t));

      final List<Record> inputRecords = test.inputs().stream()
          .map(r -> {
            final String topicName = r.topicName().toLowerCase();

            // If a schema is found for the topic, then pass it to the RecordNode to
            // parse columns which value type cannot be detected without a schema
            if (topics.containsKey(topicName)) {
              return r.build(
                  topics.get(topicName).getKeySchema(),
                  topics.get(topicName).getValueSchema(),
                  topics.get(topicName).getKeyFeatures(),
                  topics.get(topicName).getValueFeatures()
              );
            }

            return r.build();
          })
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
          topics.values(),
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
