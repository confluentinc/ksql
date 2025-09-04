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

import static io.confluent.ksql.test.utils.ImmutableCollections.immutableCopyOf;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.schema.query.QuerySchemas;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.tools.test.model.TestLocation;
import io.confluent.ksql.tools.test.model.Topic;
import io.confluent.ksql.util.KsqlConfig;
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

  private final TestLocation location;
  private final Path originalFileName;
  private final String name;
  private final VersionBounds versionBounds;
  private final Map<String, Object> properties;
  private final Collection<Topic> topics;
  private final List<Record> inputRecords;
  private final List<Record> outputRecords;
  private final List<String> statements;
  private final Optional<Matcher<Throwable>> expectedException;
  private List<String> generatedTopologies;
  private Map<String, QuerySchemas.SchemaInfo> generatedSchemas;
  private final Optional<TopologyAndConfigs> expectedTopology;
  private final PostConditions postConditions;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public TestCase(
      final TestLocation location,
      final Path originalFileName,
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
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this(
        location,
        originalFileName,
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
      final TestLocation location,
      final Path originalFileName,
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
    this.topics = immutableCopyOf(topics);
    this.originalFileName = requireNonNull(originalFileName, "originalFileName");
    this.inputRecords = ImmutableList.copyOf(inputRecords);
    this.outputRecords = ImmutableList.copyOf(outputRecords);
    this.location = requireNonNull(location, "location");
    this.name = name;
    this.versionBounds = Objects.requireNonNull(versionBounds, "versionBounds");
    this.properties = ImmutableMap.copyOf(properties);
    this.statements = ImmutableList.copyOf(statements);
    this.expectedException = requireNonNull(expectedException, "expectedException");
    this.expectedTopology = requireNonNull(expectedTopology, "expectedTopology");
    this.postConditions = Objects.requireNonNull(postConditions, "postConditions");
  }

  @Override
  public VersionBounds getVersionBounds() {
    return versionBounds;
  }

  public TestCase withExpectedTopology(
      final KsqlVersion version,
      final TopologyAndConfigs expectedTopology
  ) {
    if (!versionBounds.contains(version)) {
      throw new IllegalArgumentException("Test does not support supplied version: " + version);
    }

    final String newName = name + "-" + version.getName()
        + (version.getTimestamp().isPresent() ? "-" + version.getTimestamp().getAsLong() : "");
    final TestCase copy = new TestCase(
        location,
        originalFileName,
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

  @Override
  public String getName() {
    return name;
  }

  @Override
  public TestLocation getTestLocation() {
    return location;
  }

  public Path getOriginalFileName() {
    return originalFileName;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "topics is ImmutableCollection")
  public Collection<Topic> getTopics() {
    return topics;
  }

  public void setGeneratedTopologies(final List<String> generatedTopology) {
    this.generatedTopologies = ImmutableList.copyOf(
        Objects.requireNonNull(generatedTopology, "generatedTopology")
    );
  }

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "generatedTopologies is ImmutableList"
  )
  public List<String> getGeneratedTopologies() {
    return generatedTopologies;
  }

  public Optional<TopologyAndConfigs> getExpectedTopology() {
    return expectedTopology;
  }

  public void setGeneratedSchemas(final Map<String, QuerySchemas.SchemaInfo> generatedSchemas) {
    this.generatedSchemas = ImmutableMap.copyOf(
        Objects.requireNonNull(generatedSchemas, "generatedSchemas")
    );
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "generatedSchemas is ImmutableMap")
  public Map<String, QuerySchemas.SchemaInfo> getGeneratedSchemas() {
    return generatedSchemas;
  }

  public KsqlConfig applyPersistedProperties(final KsqlConfig systemConfig) {
    final Map<String, String> persistedConfigs = expectedTopology
        .map(TopologyAndConfigs::getConfigs)
        .orElseGet(HashMap::new);

    return persistedConfigs.isEmpty()
        ? systemConfig
        : systemConfig.overrideBreakingConfigsWithOriginalValues(persistedConfigs);
  }

  private KsqlConfig applyPropertyOverrides(final KsqlConfig sourceConfig) {
    return sourceConfig.cloneWithPropertyOverwrite(properties);
  }

  public KsqlConfig applyProperties(final KsqlConfig systemConfig) {
    return applyPropertyOverrides(applyPersistedProperties(systemConfig));
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "properties is ImmutableMap")
  public Map<String, Object> properties() {
    return properties;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "statements is ImmutableList")
  public List<String> statements() {
    return statements;
  }

  public PostConditions getPostConditions() {
    return postConditions;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "inputRecords is ImmutableList")
  public List<Record> getInputRecords() {
    return inputRecords;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "outputRecords is ImmutableList")
  public List<Record> getOutputRecords() {
    return outputRecords;
  }

  public Optional<Matcher<Throwable>> expectedException() {
    return expectedException;
  }
}