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

package io.confluent.ksql.test;

import static io.confluent.ksql.test.EndToEndEngineTestUtil.CONFIG_END_MARKER;
import static io.confluent.ksql.test.EndToEndEngineTestUtil.SCHEMAS_END_MARKER;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.test.IntegrationTest;
import org.junit.experimental.categories.Category;

/**
 * Utility to help re-write the expected topology files used by {@link QueryTranslationTest}.
 *
 * Occasionally, things change in the way KStreams generates topologies and we need to update the
 * previously saved topologies to bring them back inline.  Obviously, care should be taken when
 * doing so to ensure no backwards incompatible changes are being hidden by any changes made. *
 */
@Category(IntegrationTest.class)
public final class TopologyFileRewriter {

  /**
   * Set {@code REWRITER} to an appropriate rewriter impl.
   */
  private static final Rewriter REWRITER = new TheRewriter();

  /**
   * Exclude some versions. Anything version starting with one of these strings is excluded:
   */
  private static final Set<String> EXCLUDE_VERSIONS = ImmutableSet.<String>builder()
      //.add("5_0")
      //.add("5_1")
      .build();

  private TopologyFileRewriter() {
  }

  public static void main(final String[] args) throws Exception {
    final Path baseDir = TopologyFileGenerator.findBaseDir();

    Files.list(baseDir)
        .filter(Files::isDirectory)
        .filter(TopologyFileRewriter::includedVersion)
        .forEach(TopologyFileRewriter::rewriteToplogyDirectory);
  }

  private static boolean includedVersion(final Path path) {

    final String version = getVersion(path);

    return EXCLUDE_VERSIONS.stream()
        .noneMatch(version::startsWith);
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
  private static String getVersion(final Path versionDir) {
    try {
      final Path versionFile = versionDir.resolve(QueryTranslationTest.TOPOLOGY_VERSION_FILE);
      if (Files.exists(versionFile)) {
        return new String(Files.readAllBytes(versionFile), UTF_8);
      }

      return versionDir.getFileName().toString();
    } catch (final Exception e) {
      throw new RuntimeException("Failed to determine version in " + versionDir, e);
    }
  }

  private static void rewriteToplogyDirectory(final Path versionDir) {
    try {

      System.out.println("Starting to rewrite topology files in " + versionDir);

      Files.list(versionDir)
          .filter(Files::isRegularFile)
          .filter(path -> !path.endsWith(QueryTranslationTest.TOPOLOGY_VERSION_FILE))
          .forEach(TopologyFileRewriter::rewriteTopologyFile);

      System.out.println("Done rewriting topology files in" + versionDir);
    } catch (final Exception e) {
      throw new RuntimeException("Failed processing version dir: " + versionDir, e);
    }
  }

  private static void rewriteTopologyFile(final Path path) {
    try {

      final String content = new String(Files.readAllBytes(path), UTF_8);

      final String rewritten = REWRITER.rewrite(path, content);

      Files.write(path, rewritten.getBytes(UTF_8));

      System.out.println("Rewritten topology file: " + path);
    } catch (final Exception e) {
      throw new RuntimeException("Failed processing topology file: " + path, e);
    }
  }

  private static String grabContent(
      final String contents,
      final Optional<String> startMarker,
      final Optional<String> endMarker
  ) {
    final int start = startMarker
        .map(marker -> {
          final int idx = contents.indexOf(marker + System.lineSeparator());
          return idx < 0 ? idx : idx + marker.length() + 1;
        })
        .orElse(0);

    if (start < 0) {
      throw new RuntimeException("Failed to find marker for start of section: " + startMarker);
    }

    final int end = endMarker
        .map(contents::indexOf)
        .orElse(contents.length());

    if (end < 0) {
      throw new RuntimeException("Failed to find marker for end of section: " + startMarker);
    }

    return contents.substring(start, end);
  }

  private interface Rewriter {
    String rewrite(final Path path, final String contents);
  }

  private interface StructuredRewriter extends Rewriter {

    default String rewriteConfig(final Path path, final String configs) {
      return configs;
    }

    default String rewriteSchemas(final Path path, final String schemas) {
      return schemas;
    }

    default String rewriteTopologies(final Path path, final String topologies) {
      return topologies;
    }

    default String rewrite(final Path path, final String contents) {
      final String newConfig = rewriteConfig(path,
              grabContent(contents, Optional.empty(), Optional.of(CONFIG_END_MARKER)))
              + CONFIG_END_MARKER
              + System.lineSeparator();

      final boolean hasSchemas = contents.contains(SCHEMAS_END_MARKER);

      final String newSchemas = hasSchemas
          ? rewriteSchemas(path,
          grabContent(contents, Optional.of(CONFIG_END_MARKER), Optional.of(SCHEMAS_END_MARKER)))
          + SCHEMAS_END_MARKER
          + System.lineSeparator()
          : "";

      final Optional<String> topologyStart = hasSchemas
          ? Optional.of(SCHEMAS_END_MARKER)
          : Optional.of(CONFIG_END_MARKER);

      final String newTopologies = rewriteTopologies(path,
          grabContent(contents, topologyStart, Optional.empty()));

      return newConfig + newSchemas + newTopologies;
    }
  }

  private static final class TheRewriter implements StructuredRewriter {

    @Override
    public String rewriteSchemas(final Path path, final String schemas) {

      int start;
      String result = schemas;

      while ((start = result.indexOf("optional<")) != -1) {
        final int end = findCloseTagFor(result, start + "optional".length());

        final String contents = result.substring(start + "optional<".length(), end);

        result = result.substring(0, start)
            + contents
            + result.substring(end + 1);
      }

      return result
          .replaceAll(",(\\S)", ", $1")
          .replaceAll("\\n", " NOT NULL" + System.lineSeparator())
          .replaceAll("struct<", "STRUCT<")
          .replaceAll("map<", "MAP<")
          .replaceAll("array<", "ARRAY<")
          .replaceAll("boolean", "BOOLEAN")
          .replaceAll("int32", "INT")
          .replaceAll("int64", "BIGINT")
          .replaceAll("float64", "DOUBLE")
          .replaceAll("string", "VARCHAR");
    }

    private static int findCloseTagFor(final String contents, final int startIdx) {
      assert(contents.charAt(startIdx) == '<');

      int depth = 1;
      int idx = startIdx + 1;

      while (depth > 0 && idx < contents.length()) {
        final char c = contents.charAt(idx++);
        switch (c) {
          case '<':
            depth++;
            break;

          case '>':
            depth--;
            break;
        }
      }

      if (depth > 0) {
        throw new RuntimeException("Reached end of file before finding close tag");
      }

      return idx - 1;
    }
  }
}
