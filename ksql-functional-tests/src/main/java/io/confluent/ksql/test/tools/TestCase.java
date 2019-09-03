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
  private final Optional<KsqlVersion> ksqlVersion;
  private final Map<String, Object> properties;
  private final Collection<Topic> topics;
  private final List<Record> inputRecords;
  private final List<Record> outputRecords;
  private final List<String> statements;
  private final Optional<Matcher<Throwable>> expectedException;
  private List<String> generatedTopologies;
  private List<String> generatedSchemas;
  private Optional<TopologyAndConfigs> expectedTopology = Optional.empty();
  private final PostConditions postConditions;

  public TestCase(
      final Path testPath,
      final String name,
      final Optional<KsqlVersion> ksqlVersion,
      final Map<String, Object> properties,
      final Collection<Topic> topics,
      final List<Record> inputRecords,
      final List<Record> outputRecords,
      final List<String> statements,
      final Optional<Matcher<Throwable>> expectedException,
      final PostConditions postConditions
  ) {
    this.topics = topics;
    this.inputRecords = inputRecords;
    this.outputRecords = outputRecords;
    this.testPath = testPath;
    this.name = name;
    this.ksqlVersion = Objects.requireNonNull(ksqlVersion, "ksqlVersion");
    this.properties = ImmutableMap.copyOf(properties);
    this.statements = statements;
    this.expectedException = requireNonNull(expectedException, "expectedException");
    this.postConditions = Objects.requireNonNull(postConditions, "postConditions");
  }

  public TestCase withVersion(final KsqlVersion version) {
    final String newName = name + "-" + version.getName();
    final TestCase copy = new TestCase(
        testPath,
        newName,
        Optional.of(version),
        properties,
        topics,
        inputRecords,
        outputRecords,
        statements,
        expectedException,
        postConditions);
    copy.generatedTopologies = generatedTopologies;
    copy.expectedTopology = expectedTopology;
    copy.generatedSchemas = generatedSchemas;
    return copy;
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

  public void setExpectedTopology(final TopologyAndConfigs expectedTopology) {
    this.expectedTopology = Optional.of(expectedTopology);
  }

  public Optional<TopologyAndConfigs> getExpectedTopology() {
    return expectedTopology;
  }

  public void setGeneratedSchemas(final List<String> generatedSchemas) {
    this.generatedSchemas = Objects.requireNonNull(generatedSchemas, "generatedSchemas");
  }

  public List<String> getGeneratedSchemas() {
    return generatedSchemas;
  }

  public Map<String, String> persistedProperties() {
    return expectedTopology
        .flatMap(TopologyAndConfigs::getConfigs)
        .orElseGet(HashMap::new);
  }

  public Optional<KsqlVersion> getKsqlVersion() {
    return ksqlVersion;
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