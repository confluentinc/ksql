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

package io.confluent.ksql.test.parser;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.test.KsqlTestException;
import io.confluent.ksql.test.parser.TestDirective.Type;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The {@code SqlTestLoader} loads the test files that should be run
 * by the Ksql testing tool based on a path and optional filters.
 */
public class SqlTestLoader {

  private final Predicate<Test> shouldRun;

  public SqlTestLoader() {
    this(t -> true);
  }

  /**
   * @param testFilter filters out which tests to run
   */
  public SqlTestLoader(final Predicate<Test> testFilter) {
    this.shouldRun = Objects.requireNonNull(testFilter, "testFilter");
  }

  /**
   * @param path a directory containing all test files to run
   *
   * @return a list of tests to run
   */
  public List<Test> loadDirectory(final Path path) throws IOException {
    final SqlTestLoader loader = new SqlTestLoader();
    final List<Path> files = Files
        .find(path, Integer.MAX_VALUE, (filePath, fileAttr) -> fileAttr.isRegularFile())
        .collect(Collectors.toList());

    final ImmutableList.Builder<Test> builder = ImmutableList.builder();
    for (final Path file : files) {
      builder.addAll(loader.loadTest(file));
    }

    return builder.build();
  }

  /**
   * @param path a single sql test file, containing possibly many tests
   *
   * @return the list of tests to run
   */
  public List<Test> loadTest(final Path path) throws IOException {
    final ImmutableList.Builder<Test> builder = ImmutableList.builder();

    List<TestStatement> statements = null;
    String name = null;

    final SqlTestReader reader = SqlTestReader.of(path);
    while (reader.hasNext()) {
      final TestStatement statement = reader.next();
      final Optional<String> nextName = statement.consumeDirective(
          directive -> directive.getType() == Type.TEST ? directive.getContents() : null
      );

      if (nextName.isPresent()) {
        // flush the previous test
        if (statements != null) {
          builder.add(new Test(path, name, statements));
        }

        statements = new ArrayList<>();
        name = nextName.get();
      } else if (statements == null) {
        throw new KsqlTestException(statement, path, "Exepcted test to start with --@test.");
      }

      statements.add(statement);
    }

    builder.add(new Test(path, name, statements));
    return builder.build().stream().filter(shouldRun).collect(ImmutableList.toImmutableList());
  }

  /**
   * Represents a tuple of (test name, file, test statements) that constitute a ksql
   * test.
   */
  public static class Test {

    private final Path file;
    private final String name;
    private final List<TestStatement> statements;

    public Test(final Path file, final String name, final List<TestStatement> statements) {
      this.file = file;
      this.name = name;
      this.statements = statements;
    }

    public Path getFile() {
      return file;
    }

    public String getName() {
      return name;
    }

    public List<TestStatement> getStatements() {
      return statements;
    }

    /**
     * @return an {@code Object[]} representation of this class used for Parameterized
     *         JUnit testing. The representation is [name, file, statements]
     */
    public Object[] asObjectArray() {
      return new Object[]{name, file, statements};
    }
  }

}
