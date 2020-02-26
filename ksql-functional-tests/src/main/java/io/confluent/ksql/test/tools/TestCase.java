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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.hamcrest.Matcher;

@SuppressWarnings("WeakerAccess")
public class TestCase implements VersionedTest {

  private final Path testPath;
  private final String name;
  private final VersionBounds versionBounds;
  private final Map<String, Object> properties;
  private final Collection<Topic> topics;
  private final List<Record> inputRecords;
  private final List<Record> outputRecords;
  private final List<String> statements;
  private final Optional<Matcher<Throwable>> expectedException;
  private List<String> generatedTopologies;
  private Map<String, String> generatedSchemas;
  private final Optional<TopologyAndConfigs> expectedTopology;
  private final PostConditions postConditions;

  public TestCase(
      final Path testPath,
      final String name,
      final VersionBounds versionBounds,
      final Map<String, Object> properties,
      final Collection<Topic> topics,
      final List<Record> inputRecords,
      final List<Record> outputRecords,
      final List<String> statements,
      final Optional<Matcher<Throwable>> expectedException,
      final PostConditions postConditions
  ) {
    this(
        testPath,
        name,
        versionBounds,
        properties,
        topics,
        inputRecords,
        outputRecords,
        statements,
        expectedException,
        postConditions,
        Optional.empty()
    );
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  private TestCase(
      final Path testPath,
      final String name,
      final VersionBounds versionBounds,
      final Map<String, Object> properties,
      final Collection<Topic> topics,
      final List<Record> inputRecords,
      final List<Record> outputRecords,
      final List<String> statements,
      final Optional<Matcher<Throwable>> expectedException,
      final PostConditions postConditions,
      final Optional<TopologyAndConfigs> expectedTopology
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.topics = topics;
    this.inputRecords = inputRecords;
    this.outputRecords = outputRecords;
    this.testPath = testPath;
    this.name = name;
    this.versionBounds = Objects.requireNonNull(versionBounds, "versionBounds");
    this.properties = ImmutableMap.copyOf(properties);
    this.statements = statements;
    this.expectedException = requireNonNull(expectedException, "expectedException");
    this.expectedTopology = requireNonNull(expectedTopology, "expectedTopology");
    this.postConditions = Objects.requireNonNull(postConditions, "postConditions");
  }

  @Override
  public VersionBounds getVersionBounds() {
    return versionBounds;
  }

  public TestCase withPlan(
      final KsqlVersion version,
      final TopologyAndConfigs expectedTopology,
      final List<Record> inputRecords,
      final List<Record> outputRecords
  ) {
    if (!versionBounds.contains(version)) {
      throw new IllegalArgumentException("Test does not support supplied version: " + version);
    }

    final String newName = name + "-" + version.getName()
        + (version.getTimestamp().isPresent() ? "-" + version.getTimestamp().getAsLong() : "");
    final TestCase copy = new TestCase(
        testPath,
        newName,
        versionBounds,
        properties,
        topics,
        inputRecords,
        outputRecords,
        statements,
        expectedException,
        postConditions,
        Optional.of(expectedTopology)
    );

    copy.generatedTopologies = generatedTopologies;
    copy.generatedSchemas = generatedSchemas;
    return copy;
  }

  public TestCase withExpectedTopology(
      final KsqlVersion version,
      final TopologyAndConfigs expectedTopology
  ) {
    return withPlan(version, expectedTopology, inputRecords, outputRecords);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getTestFile() {
    return testPath.toString();
  }

  public Collection<Topic> getTopics() {
    return topics;
  }

  public void setGeneratedTopologies(final List<String> generatedTopology) {
    this.generatedTopologies = Objects.requireNonNull(generatedTopology, "generatedTopology");
  }

  public List<String> getGeneratedTopologies() {
    return generatedTopologies;
  }

  public Optional<TopologyAndConfigs> getExpectedTopology() {
    return expectedTopology;
  }

  public void setGeneratedSchemas(final Map<String, String> generatedSchemas) {
    this.generatedSchemas = ImmutableMap.copyOf(
        Objects.requireNonNull(generatedSchemas, "generatedSchemas"));
  }

  public Map<String, String> getGeneratedSchemas() {
    return generatedSchemas;
  }

  public Map<String, String> persistedProperties() {
    return expectedTopology
        .map(TopologyAndConfigs::getConfigs)
        .orElseGet(HashMap::new);
  }

  public Map<String, Object> properties() {
    return properties;
  }

  public List<String> statements() {
    return statements;
  }

  public PostConditions getPostConditions() {
    return postConditions;
  }

  public List<Record> getInputRecords() {
    return inputRecords;
  }

  public List<Record> getOutputRecords() {
    return outputRecords;
  }

  public Optional<Matcher<Throwable>> expectedException() {
    return expectedException;
  }
}