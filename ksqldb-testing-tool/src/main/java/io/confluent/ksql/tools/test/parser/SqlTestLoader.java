/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.tools.test.parser;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.tools.test.KsqlTestException;
import io.confluent.ksql.tools.test.TestLoader;
import io.confluent.ksql.tools.test.model.Test;
import io.confluent.ksql.tools.test.model.TestLocation;
import io.confluent.ksql.tools.test.parser.SqlTestLoader.SqlTest;
import io.confluent.ksql.tools.test.parser.TestDirective.Type;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@code SqlTestLoader} loads the test files that should be run
 * by the Ksql testing tool based on a path and optional filters.
 */
public class SqlTestLoader implements TestLoader<SqlTest> {

  private final Predicate<SqlTest> shouldRun;
  private final Path path;

  public SqlTestLoader(final Path path) {
    this(t -> true, path);
  }

  /**
   * @param testFilter filters out which tests to run
   * @param path       the top-level dir to load
   */
  public SqlTestLoader(final Predicate<SqlTest> testFilter, final Path path) {
    this.shouldRun = Objects.requireNonNull(testFilter, "testFilter");
    this.path = Objects.requireNonNull(path, "path");
  }

  @Override
  public Stream<SqlTest> load() throws IOException {
    final List<Path> files = Files
        .find(path, Integer.MAX_VALUE, (filePath, fileAttr) -> fileAttr.isRegularFile())
        .collect(Collectors.toList());

    final ImmutableList.Builder<SqlTest> builder = ImmutableList.builder();
    final List<String> whiteList = TestLoader.getWhiteList();
    for (final Path file : files) {
      if (whiteList.isEmpty() || whiteList.stream().anyMatch(file::endsWith)) {
        builder.addAll(loadTest(file));
      }
    }

    return builder.build().stream();
  }

  /**
   * @param path a single sql test file, containing possibly many tests
   *
   * @return the list of tests to run
   */
  public List<SqlTest> loadTest(final Path path) throws IOException {
    return loadTest(SqlTestReader.of(path), path, shouldRun);
  }

  /**
   * @param test a single sql test, containing possibly many tests
   *
   * @return the list of tests to run
   */
  public static List<SqlTest> loadTest(final String test)  {
    return loadTest(SqlTestReader.of(test), null, t -> true);
  }

  private static List<SqlTest> loadTest(
      final SqlTestReader reader,
      final Path path,
      final Predicate<SqlTest> shouldRun
  ) {
    final ImmutableList.Builder<SqlTest> builder = ImmutableList.builder();

    List<TestStatement> statements = null;
    String name = null;

    while (reader.hasNext()) {
      final TestStatement statement = reader.next();
      final Optional<String> nextName = statement.consumeDirective(
          directive -> directive.getType() == Type.TEST ? directive.getContents() : null
      );

      if (nextName.isPresent()) {
        // flush the previous test
        if (statements != null) {
          builder.add(new SqlTest(path, name, statements));
        }

        statements = new ArrayList<>();
        name = nextName.get();
      } else if (statements == null) {
        throw new KsqlTestException(statement, path, "Exepcted test to start with --@test.");
      }

      statements.add(statement);
    }

    builder.add(new SqlTest(path, name, statements));
    return builder.build().stream().filter(shouldRun).collect(ImmutableList.toImmutableList());
  }

  /**
   * Represents a tuple of (test name, file, test statements) that constitute a ksql
   * test.
   */
  public static class SqlTest implements Test {

    private final Path file;
    private final String name;
    private final List<TestStatement> statements;

    public SqlTest(final Path file, final String name, final List<TestStatement> statements) {
      this.file = file;
      this.name = name;
      this.statements = ImmutableList.copyOf(statements);
    }

    public Path getFile() {
      return file;
    }

    public String getName() {
      return name;
    }

    @Override
    public TestLocation getTestLocation() {
      return () -> file;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "statements is ImmutableList")
    public List<TestStatement> getStatements() {
      return statements;
    }
  }

}
