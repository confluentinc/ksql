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
import io.confluent.ksql.model.SemanticVersion;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.tools.TopologyAndConfigs;
import io.confluent.ksql.test.tools.VersionedTest;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
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

  public static void writeExpectedTopologyFile(
      final String queryName,
      final PersistentQueryMetadata query,
      final Map<String, String> configs,
      final ObjectWriter objectWriter,
      final Path topologyDir
  ) {
    try {
      final String updatedQueryName = formatQueryName(queryName);
      final Path topologyFile = topologyDir.resolve(updatedQueryName);
      final String configString = objectWriter.writeValueAsString(configs);
      final String topologyString = query.getTopology().describe().toString();
      final String schemasString = query.getSchemasDescription();

      final byte[] topologyBytes =
          (configString + "\n"
              + CONFIG_END_MARKER + "\n"
              + schemasString + "\n"
              + SCHEMAS_END_MARKER + "\n"
              + topologyString
          ).getBytes(StandardCharsets.UTF_8);

      Files.write(topologyFile,
          topologyBytes,
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING);
    } catch (final IOException e) {
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
        : Arrays.stream(versionProp.split(TOPOLOGY_VERSIONS_DELIMITER));

    return versionStrings
        .map(this::getVersion)
        .collect(Collectors.toList());
  }

  private List<String> findExpectedTopologyDirectories() {
    try {
      return findContentsOfDirectory(topologyChecksDir);
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
    builder = builder.add(test);

    for (final TopologiesAndVersion topologies : expectedTopologies) {
      final TopologyAndConfigs topologyAndConfigs =
          topologies.getTopology(formatQueryName(test.getName()));
      // could be null if the testCase has expected errors, no topology or configs saved
      if (topologyAndConfigs != null) {
        final T versionedTest = (T) test.withVersion(topologies.getVersion());
        versionedTest.setExpectedTopology(topologyAndConfigs);
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

  public static TopologyAndConfigs readTopologyFile(
      final String file,
      final ObjectReader objectReader
  ) {
    final InputStream s = ExpectedTopologiesTestLoader.class.getClassLoader()
        .getResourceAsStream(file);
    if (s == null) {
      throw new AssertionError("File not found: " + file);
    }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(s, UTF_8))
    ) {
      final StringBuilder topologyFileBuilder = new StringBuilder();

      String schemas = null;
      String topologyAndConfigLine;
      Optional<Map<String, String>> persistedConfigs = Optional.empty();

      while ((topologyAndConfigLine = reader.readLine()) != null) {
        if (topologyAndConfigLine.contains(CONFIG_END_MARKER)) {
          persistedConfigs = Optional
              .of(objectReader.readValue(topologyFileBuilder.toString()));
          topologyFileBuilder.setLength(0);
        } else if (topologyAndConfigLine.contains(SCHEMAS_END_MARKER)) {
          schemas = StringUtils.stripEnd(topologyFileBuilder.toString(), "\n");
          topologyFileBuilder.setLength(0);
        } else {
          topologyFileBuilder.append(topologyAndConfigLine).append("\n");
        }
      }

      return new TopologyAndConfigs(
          topologyFileBuilder.toString(),
          Optional.ofNullable(schemas),
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
        .replaceAll(" - (AVRO|JSON)$", "")
        .replaceAll("\\s|/", "_");
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