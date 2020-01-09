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

package io.confluent.ksql.test.loader;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.model.SemanticVersion;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.tools.TopologyAndConfigs;
import io.confluent.ksql.test.tools.VersionedTest;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

/**
 * Loads the expected topology files for each test and creates a new test for each expected version
 * and sets the expected topology of the test.
 */
public class ExpectedTopologiesTestLoader<T extends VersionedTest> implements TestLoader<T> {

  public static final String TOPOLOGY_VERSION_FILE = "__version";
  public static final String CONFIG_END_MARKER = "CONFIGS_END";
  public static final String SCHEMAS_END_MARKER = "SCHEMAS_END";

  private static final Pattern TOPOLOGY_VERSION_PATTERN = Pattern.compile("(\\d+)_(\\d+)(_\\d+)?");
  private static final String TOPOLOGY_VERSIONS_DELIMITER = ",";
  private static final String TOPOLOGY_VERSIONS_PROP = "topology.versions";
  private static final String TOPOLOGY_VERSION_LATEST = "latest-only";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final KsqlVersion CURRENT_VERSION = KsqlVersion.current();
  private static final String INVALID_FILENAME_CHARS_PATTERN = "\\s|/|\\\\|:|\\*|\\?|\"|<|>|\\|";

  private final String topologyChecksDir;
  private final TestLoader<T> innerLoader;

  public static <T extends VersionedTest> ExpectedTopologiesTestLoader<T> of(
      final TestLoader<T> innerLoader,
      final String topologyChecksDir
  ) {
    return new ExpectedTopologiesTestLoader<>(innerLoader, topologyChecksDir);
  }

  private ExpectedTopologiesTestLoader(
      final TestLoader<T> innerLoader,
      final String topologyChecksDir
  ) {
    this.topologyChecksDir = Objects.requireNonNull(topologyChecksDir, "topologyChecksDir");
    this.innerLoader = Objects.requireNonNull(innerLoader, "innerLoader");
  }

  public Stream<T> load() {
    final List<TopologiesAndVersion> expectedTopologies = loadTopologiesAndVersions();

    return innerLoader.load()
        .flatMap(q -> buildVersionedTestCases(q, expectedTopologies));
  }

  public static Path buildExpectedTopologyPath(final String queryName, final Path topologyDir) {
    final String updatedQueryName = formatQueryName(queryName);
    return topologyDir.resolve(updatedQueryName);
  }

  public static String buildExpectedTopologyContent(
      final PersistentQueryMetadata query,
      final Map<String, String> configs
  ) {
    try {
      final ObjectWriter objectWriter = OBJECT_MAPPER.writerWithDefaultPrettyPrinter();

      final String configString = objectWriter.writeValueAsString(configs);
      final String topologyString = query.getTopology().describe().toString();
      final String schemasString = query.getSchemasString();

      return configString + "\n"
          + CONFIG_END_MARKER + "\n"
          + schemasString + "\n"
          + SCHEMAS_END_MARKER + "\n"
          + topologyString;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<TopologiesAndVersion> loadTopologiesAndVersions() {
    return getTopologyVersions().stream()
        .map(version -> new TopologiesAndVersion(
            version,
            loadExpectedTopologies(topologyChecksDir + version.getName())
        ))
        .collect(Collectors.toList());
  }

  private List<KsqlVersion> getTopologyVersions() {
    final String versionProp = System.getProperty(TOPOLOGY_VERSIONS_PROP, "");

    final Stream<String> versionStrings = versionProp.isEmpty()
        ? findExpectedTopologyDirectories().stream()
        : versionProp.equalsIgnoreCase(TOPOLOGY_VERSION_LATEST)
            ? Stream.of()
            : Arrays.stream(versionProp.split(TOPOLOGY_VERSIONS_DELIMITER));

    return versionStrings
        .map(this::getVersion)
        .collect(Collectors.toList());
  }

  private List<String> findExpectedTopologyDirectories() {
    try {
      return findContentsOfDirectory(topologyChecksDir).stream()
          .filter(file -> !file.endsWith(".md"))
          .collect(Collectors.toList());
    } catch (final Exception e) {
      throw new RuntimeException("Could not find expected topology directories.", e);
    }
  }

  private KsqlVersion getVersion(final String dir) {
    final Path versionFile = Paths.get(topologyChecksDir, dir, TOPOLOGY_VERSION_FILE);

    try {
      final String versionString = loadContents(versionFile.toString())
          .map(content -> String.join("", content))
          .orElse(dir);

      final Matcher matcher = TOPOLOGY_VERSION_PATTERN.matcher(versionString);
      if (!matcher.matches()) {
        throw new RuntimeException("Version does not match required pattern. "
            + TOPOLOGY_VERSION_PATTERN
            + ". Correct the directory name, or add a " + TOPOLOGY_VERSION_FILE + ".");
      }

      final int major = Integer.parseInt(matcher.group(1));
      final int minor = Integer.parseInt(matcher.group(2));
      final int patch = matcher.groupCount() == 3
          ? 0
          : Integer.parseInt(matcher.group(3).substring(1));

      return KsqlVersion.of(dir, SemanticVersion.of(major, minor, patch));
    } catch (Exception e) {
      throw new RuntimeException("Failed to load version file: " + versionFile, e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends VersionedTest> Stream<T> buildVersionedTestCases(
      final T test,
      final List<TopologiesAndVersion> expectedTopologies
  ) {
    Stream.Builder<T> builder = Stream.builder();

    for (final TopologiesAndVersion topologies : expectedTopologies) {
      if (!test.getVersionBounds().contains(topologies.getVersion())) {
        continue;
      }

      final TopologyAndConfigs topologyAndConfigs =
          topologies.getTopology(formatQueryName(test.getName()));
      // could be null if the testCase has expected errors, no topology or configs saved
      if (topologyAndConfigs != null) {
        final T versionedTest = (T) test.withExpectedTopology(
            topologies.getVersion(),
            topologyAndConfigs
        );

        builder = builder.add(versionedTest);
      }
    }
    return builder.build();
  }

  private static Map<String, TopologyAndConfigs> loadExpectedTopologies(final String dir) {
    final HashMap<String, TopologyAndConfigs> expectedTopologyAndConfigs = new HashMap<>();
    final ObjectReader objectReader = new ObjectMapper().readerFor(Map.class);
    final List<String> topologyFiles = findExpectedTopologyFiles(dir);
    topologyFiles.forEach(fileName -> {
      final TopologyAndConfigs topologyAndConfigs = readTopologyFile(dir + "/" + fileName,
          objectReader);
      expectedTopologyAndConfigs.put(fileName, topologyAndConfigs);
    });
    return expectedTopologyAndConfigs;
  }

  private static List<String> findExpectedTopologyFiles(final String dir) {
    try {
      return findContentsOfDirectory(dir);
    } catch (final Exception e) {
      throw new RuntimeException("Could not find expected topology files. dir: " + dir, e);
    }
  }

  private static Map<String, String> parseSchemas(final String asString) {
    if (asString == null) {
      return Collections.emptyMap();
    }
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    final List<String> lines = Arrays.asList(asString.split("\n"));
    for (final String line : lines) {
      final String[] split = line.split(" *= *");
      if (split.length != 2) {
        throw new RuntimeException("Unexpected format for schema string");
      }
      builder.put(split[0], split[1]);
    }
    return builder.build();
  }

  private static TopologyAndConfigs readTopologyFile(
      final String file,
      final ObjectReader objectReader
  ) {
    final InputStream s = ExpectedTopologiesTestLoader.class.getClassLoader()
        .getResourceAsStream(file);
    if (s == null) {
      throw new AssertionError("Resource not found: " + file);
    }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(s, UTF_8))
    ) {
      final StringBuilder topologyFileBuilder = new StringBuilder();

      String schemas = null;
      String topologyAndConfigLine;
      Map<String, String> persistedConfigs = Collections.emptyMap();

      while ((topologyAndConfigLine = reader.readLine()) != null) {
        if (topologyAndConfigLine.contains(CONFIG_END_MARKER)) {
          persistedConfigs = objectReader.readValue(topologyFileBuilder.toString());
          topologyFileBuilder.setLength(0);
        } else if (topologyAndConfigLine.contains(SCHEMAS_END_MARKER)) {
          schemas = StringUtils.stripEnd(topologyFileBuilder.toString(), "\n");
          topologyFileBuilder.setLength(0);
        } else {
          topologyFileBuilder.append(topologyAndConfigLine).append("\n");
        }
      }

      return new TopologyAndConfigs(
          Optional.empty(),
          topologyFileBuilder.toString(),
          parseSchemas(schemas),
          persistedConfigs
      );

    } catch (final IOException e) {
      throw new RuntimeException(String.format("Couldn't read topology file %s %s", file, e));
    }
  }

  private static Optional<List<String>> loadContents(final String path) {
    final InputStream s = ExpectedTopologiesTestLoader.class.getClassLoader()
        .getResourceAsStream(path);

    if (s == null) {
      return Optional.empty();
    }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(s, UTF_8))) {
      final List<String> contents = new ArrayList<>();
      String file;
      while ((file = reader.readLine()) != null) {
        contents.add(file);
      }
      return Optional.of(contents);
    } catch (final IOException e) {
      throw new AssertionError("Failed to read path: " + path, e);
    }
  }

  private static List<String> findContentsOfDirectory(final String path) {
    return loadContents(path)
        .orElseThrow(() -> new AssertionError("Dir not found: " + path));
  }

  private static String formatQueryName(final String originalQueryName) {
    return originalQueryName
        .replaceAll(" - (AVRO|JSON|DELIMITED|KAFKA)$", "")
        .replaceAll(INVALID_FILENAME_CHARS_PATTERN, "_");
  }

  private static class TopologiesAndVersion {

    private final KsqlVersion version;
    private final Map<String, TopologyAndConfigs> topologies;

    TopologiesAndVersion(final KsqlVersion version,
        final Map<String, TopologyAndConfigs> topologies) {
      this.version = Objects.requireNonNull(version, "version");
      this.topologies = Objects.requireNonNull(topologies, "topologies");
    }

    KsqlVersion getVersion() {
      return version;
    }

    TopologyAndConfigs getTopology(final String name) {
      return topologies.get(name);
    }
  }
}