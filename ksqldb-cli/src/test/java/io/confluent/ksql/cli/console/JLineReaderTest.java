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

package io.confluent.ksql.cli.console;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.confluent.ksql.test.util.KsqlTestFolder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.jline.reader.EndOfFileException;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JLineReaderTest {

  @Rule
  public TemporaryFolder tempFolder = KsqlTestFolder.temporaryFolder();

  @Mock
  private Predicate<String> cliLinePredicate;

  @Before
  public void setUp() {
    when(cliLinePredicate.test(any())).thenReturn(false);
  }

  @Test
  public void shouldSaveCommandsWithLeadingSpacesToHistory() throws IOException {
    // Given:
    final String input = "  show streams;\n";
    final JLineReader reader = createReaderForInput(input);

    // When:
    reader.readLine();

    // Then:
    assertThat(getHistory(reader), contains(input.trim()));
  }

  @Test
  public void shouldExpandInlineMacro() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("csas\t* FROM Blah;\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("CREATE STREAM s AS SELECT * FROM Blah;"));
  }

  @Test
  public void shouldExpandHistoricalLine() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo;\n bar;\n  baz; \n!2\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo;", "bar;", "baz;", "bar;"));
  }

  @Test
  public void shouldExpandRelativeLine() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo;\n bar;\n  baz; \n!-3\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo;", "bar;", "baz;", "foo;"));
  }

  @Test
  public void shouldNotExpandHistoryUnlessAtStartOfLine() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo;\n bar;\n  baz; \n !2;\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo;", "bar;", "baz;", "!2;"));
  }

  @Test
  public void shouldExpandHistoricalSearch() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo;\n bar;\n  baz; \n!?ba\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo;", "bar;", "baz;", "baz;"));
  }

  @Test
  public void shouldExpandLastLine() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo;\n bar;\n  baz; \n!!\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo;", "bar;", "baz;", "baz;"));
  }

  @Test
  public void shouldExpandHistoricalLineWithReplacement() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo;\n select col1, col2 from d; \n^col2^xyz^\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo;", "select col1, col2 from d;", "select col1, xyz from d;"));
  }

  @Test
  public void shouldHandleSingleLine() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("select * from foo;\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("select * from foo;"));
  }

  @Test
  public void shouldHandleMultiLineUsingContinuationChar() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput(
        "select * \\\n"
            + "from foo;\n"
    );

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("select * from foo;"));
  }

  @Test
  public void shouldHandleMultiLineWithoutContinuationChar() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput(
        "select *\n\t"
            + "from foo;\n"
    );

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("select *\nfrom foo;"));
  }

  @Test
  public void shouldHandleMultiLineWithOpenQuotes() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput(
        "select * 'string that ends in termination char;\n"
            + "' from foo;\n"
    );

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("select * 'string that ends in termination char;\n' from foo;"));
  }

  @Test
  public void shouldHandleMultiLineWithComments() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput(
        "-- first inline comment\n"
            + "select * '-- not comment\n"
            + "' -- second inline comment\n"
            + "from foo; -- third inline comment\n"
            + "-- forth inline comment\n"
    );

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains(
        "-- first inline comment",
        "select * '-- not comment\n' -- second inline comment\nfrom foo; -- third inline comment",
        "-- forth inline comment"
    ));
  }

  @Test
  public void shouldHandleCliCommandsWithInlineComments() throws Exception {
    // Given:
    when(cliLinePredicate.test("Exit")).thenReturn(true);
    final JLineReader reader = createReaderForInput(
        "-- first inline comment\n"
            + "Exit -- second inline comment\n"
            + " -- third inline comment\n"
    );

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains(
        "-- first inline comment",
        "Exit -- second inline comment",
        "-- third inline comment"
    ));
  }

  @SuppressWarnings("InfiniteLoopStatement")
  private static List<String> readAllLines(final JLineReader reader) {
    final List<String> commands = new ArrayList<>();
    try {
      while (true) {
         final String line = reader.readLine();
         commands.add(line.trim());
      }
    } catch (final EndOfFileException e) {
      // this indicates end of input in JLine
    }
    return commands;
  }

  private static List<String> getHistory(final JLineReader reader) {
    final List<String> commands = new ArrayList<>();
    reader.getHistory().forEach(entry -> commands.add(entry.line()));
    return commands;
  }

  private JLineReader createReaderForInput(final String input) throws IOException {
    final InputStream inputStream =
        new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    final OutputStream outputStream = new ByteArrayOutputStream(512);
    final Terminal terminal = new DumbTerminal(inputStream, outputStream);
    final File tempHistoryFile = tempFolder.newFile("ksql-history.txt");
    final Path historyFilePath = Paths.get(tempHistoryFile.getAbsolutePath());
    return new JLineReader(terminal, historyFilePath, cliLinePredicate);
  }
}
