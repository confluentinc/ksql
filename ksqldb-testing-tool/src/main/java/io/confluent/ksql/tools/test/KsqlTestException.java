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

package io.confluent.ksql.tools.test;

import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.AssertStatement;
import io.confluent.ksql.tools.test.model.LocationWithinFile;
import io.confluent.ksql.tools.test.parser.TestDirective;
import io.confluent.ksql.tools.test.parser.TestStatement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.ParserUtil;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

/**
 * Indicates a test exception as well as where it occurred. All sql-driven
 * tests should throw this exception at the top-level if possible in order
 * to automatically populate the statement that produced it as well as the
 * location in the file.
 */
public class KsqlTestException extends KsqlException {

  public KsqlTestException(
      final TestStatement statement,
      final Path file,
      final Throwable cause
  ) {
    super(getMessage(statement, cause.getMessage(), file), cause);
  }

  public KsqlTestException(
      final TestStatement statement,
      final Path file,
      final String message
  ) {
    super(getMessage(statement, message, file));
  }

  private static String getMessage(
      final TestStatement stmt,
      final String message,
      final Path file
  ) {
    final Path srcFile = file == null ? null : toSourcePath(file);

    return stmt.apply(
        parsed -> engineMessage(parsed, message, srcFile),
        assertStatement -> assertMessage(assertStatement, message, srcFile),
        directive -> directiveMessage(directive, message, srcFile)
    );
  }

  private static String engineMessage(
      final ParsedStatement parsedStatement,
      final String message,
      final Path file
  ) {
    final Optional<NodeLocation> loc = ParserUtil.getLocation(
        parsedStatement.getStatement());

    return String.format(
        "Test failure for statement `%s` (%s):%n\t%s%n\t%s",
        parsedStatement.getMaskedStatementText(),
        loc.map(NodeLocation::toString).orElse("unknown"),
        message,
        getFileLocation(
            file,
            loc.map(NodeLocation::getStartLineNumber).orElse(1))
    );
  }

  private static String assertMessage(
      final AssertStatement assertStatement,
      final String message,
      final Path file
  ) {
    return String.format(
        "Test failure for assert `%s` (%s):%n\t%s%n\t%s",
        SqlFormatter.formatSql(assertStatement),
        assertStatement.getLocation().map(Objects::toString).orElse("unknown"),
        message,
        getFileLocation(
            file,
            assertStatement.getLocation().map(NodeLocation::getStartLineNumber).orElse(1))
    );
  }

  private static String directiveMessage(
      final TestDirective directive,
      final String message,
      final Path file
  ) {
    return String.format(
        "Test failure during directive evaluation `%s` (%s):%n\t%s%s",
        directive,
        directive.getLocation(),
        message,
        getFileLocation(file, directive.getLocation().getStartLineNumber())
    );
  }

  private static String getFileLocation(final Path file, final int startLineNumber) {
    return file == null
        ? ""
        : "%n\t" + new LocationWithinFile(file, startLineNumber).toString();
  }

  /**
   * Convert path from under {@code target/test-classes} to source path
   */
  private static Path toSourcePath(final Path path) {
    final Path converted = Paths.get(path.toAbsolutePath().toString()
        .replace("/target/test-classes", "/src/test/resources/"));

    return Files.exists(converted)
        ? converted
        : path;
  }
}
