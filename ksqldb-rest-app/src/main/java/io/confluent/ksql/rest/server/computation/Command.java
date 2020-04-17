/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Version;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.maven.artifact.versioning.ArtifactVersion;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;

@JsonSubTypes({})
public class Command {

  private static final VersionChecker VERSION_CHECK = new VersionChecker(Version::getVersion);

  private final String statement;
  private final Map<String, Object> overwriteProperties;
  private final Map<String, String> originalProperties;
  private final Optional<KsqlPlan> plan;
  private final Optional<String> version;

  @JsonCreator
  public Command(
      @JsonProperty(value = "statement", required = true) final String statement,
      @JsonProperty("streamsProperties") final Optional<Map<String, Object>> overwriteProperties,
      @JsonProperty("originalProperties") final Optional<Map<String, String>> originalProperties,
      @JsonProperty("plan") final Optional<KsqlPlan> plan,
      @JsonProperty("version") final Optional<String> version
  ) {
    this(
        statement,
        overwriteProperties.orElseGet(ImmutableMap::of),
        originalProperties.orElseGet(ImmutableMap::of),
        plan,
        version
    );
  }

  public Command(
      final String statement,
      final Map<String, Object> overwriteProperties,
      final Map<String, String> originalProperties,
      final Optional<KsqlPlan> plan
  ) {
    this(
        statement,
        overwriteProperties,
        originalProperties,
        plan,
        Optional.of(Version.getVersion())
    );
  }

  @VisibleForTesting
  Command(
      final String statement,
      final Map<String, Object> overwriteProperties,
      final Map<String, String> originalProperties,
      final Optional<KsqlPlan> plan,
      final Optional<String> version
  ) {
    this.statement = requireNonNull(statement, "statement");
    this.overwriteProperties = Collections.unmodifiableMap(
        requireNonNull(overwriteProperties, "overwriteProperties"));
    this.originalProperties = Collections.unmodifiableMap(
        requireNonNull(originalProperties, "originalProperties"));
    this.plan = requireNonNull(plan, "plan");
    this.version = requireNonNull(version, "version");

    version.ifPresent(VERSION_CHECK::throwOnIncompatibleCommandVersion);
  }

  public String getStatement() {
    return statement;
  }

  @JsonProperty("streamsProperties")
  public Map<String, Object> getOverwriteProperties() {
    return Collections.unmodifiableMap(overwriteProperties);
  }

  public Map<String, String> getOriginalProperties() {
    return originalProperties;
  }

  public Optional<KsqlPlan> getPlan() {
    return plan;
  }

  public Optional<String> getVersion() {
    return version;
  }

  public static Command of(final ConfiguredKsqlPlan configuredPlan) {
    return new Command(
        configuredPlan.getPlan().getStatementText(),
        configuredPlan.getOverrides(),
        configuredPlan.getConfig().getAllConfigPropsWithSecretsObfuscated(),
        Optional.of(configuredPlan.getPlan()),
        Optional.of(Version.getVersion())
    );
  }

  public static Command of(final ConfiguredStatement<?> configuredStatement) {
    return new Command(
        configuredStatement.getStatementText(),
        configuredStatement.getConfigOverrides(),
        configuredStatement.getConfig().getAllConfigPropsWithSecretsObfuscated(),
        Optional.empty(),
        Optional.of(Version.getVersion())
    );
  }

  @Override
  public boolean equals(final Object o) {
    return
        o instanceof Command
        && Objects.equals(statement, ((Command)o).statement)
        && Objects.equals(overwriteProperties, ((Command)o).overwriteProperties)
        && Objects.equals(originalProperties, ((Command)o).originalProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, overwriteProperties, originalProperties);
  }

  @Override
  public String toString() {
    return "Command{"
        + "statement='" + statement + '\''
        + ", overwriteProperties=" + overwriteProperties
        + ", version=" + version
        + '}';
  }

  @VisibleForTesting
  static class VersionChecker {

    private static final RangeMap<ArtifactVersion, Range<ArtifactVersion>> DB_COMPAT_MATRIX =
        ImmutableRangeMap.<ArtifactVersion, Range<ArtifactVersion>>builder()
            // versions less than 0.9.0 did not have any compatibility requirements
            // so we allow all commands for backwards compatibility reasons
            .put(Range.lessThan(ver("0.9.0")), Range.all())
            // version 0.9.0 is the most recent version and there have not been any
            // backwards incompatible changes since
            .put(Range.singleton(ver("0.9.0")), Range.all())
            .build();

    private static final RangeMap<ArtifactVersion, Range<ArtifactVersion>> CP_COMPAT_MATRIX =
        ImmutableRangeMap.<ArtifactVersion, Range<ArtifactVersion>>builder()
            // versions less than 6.0.0 did not have any compatibility requirements
            // so we allow all commands for backwards compatibility reasons
            .put(Range.lessThan(ver("6.0.0")), Range.all())
            // version 6.0.0 is the most recent version and there have not been any
            // backwards incompatible changes since
            .put(Range.singleton(ver("6.0.0")), Range.all())
            .build();

    private final RangeMap<ArtifactVersion, Range<ArtifactVersion>> compatMatrix;
    private final Supplier<String> serverVersion;

    @VisibleForTesting
    VersionChecker(
        final RangeMap<ArtifactVersion, Range<ArtifactVersion>> compatMatrix,
        final Supplier<String> serverVersion
    ) {
      this.compatMatrix = requireNonNull(compatMatrix, "compatMatrix");
      this.serverVersion = requireNonNull(serverVersion, "serverVersion");
    }

    VersionChecker(final Supplier<String> serverVersion) {
      // prefer the ksqlDB versioning scheme to the CP versioning scheme
      this(
          DB_COMPAT_MATRIX.get(ver(Version.getVersion())) == null
              ? CP_COMPAT_MATRIX
              : DB_COMPAT_MATRIX,
          serverVersion
      );
    }

    void throwOnIncompatibleCommandVersion(final String commandVersionString) {
      final ArtifactVersion cVer = ver(commandVersionString);
      final ArtifactVersion sVer = ver(serverVersion.get());

      final Range<ArtifactVersion> supportedCommandVersions = compatMatrix.get(sVer);
      if (supportedCommandVersions == null) {
        throw new IllegalStateException("Add version " + sVer + " to the compatibility matrix.");
      }

      if (!supportedCommandVersions.contains(cVer)) {
        throw new KsqlException(
            "Incompatible server and command versions. Server " + sVer + ". Command: " + cVer);
      }
    }

    private static ArtifactVersion ver(final String ver) {
      return new DefaultArtifactVersion(ver);
    }

  }
}
