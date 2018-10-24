package io.confluent.ksql.cli.console;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
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
import org.jline.reader.EndOfFileException;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JLineReaderTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void shouldSaveCommandsWithLeadingSpacesToHistory() throws IOException {
    // Given:
    final String input = "  show streams\n";
    final JLineReader reader = createReaderForInput(input);

    // When:
    reader.readLine();

    // Then:
    assertThat(getHistory(reader), contains(input.trim()));
  }

  @Test
  public void shouldExpandInlineMacro() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("csas\t\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("CREATE STREAM s AS SELECT"));
  }

  @Test
  public void shouldExpandHistoricalLine() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo\n bar\n  baz \n!2\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo", "bar", "baz", "bar"));
  }

  @Test
  public void shouldExpandRelativeLine() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo\n bar\n  baz \n!-3\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo", "bar", "baz", "foo"));
  }

  @Test
  public void shouldNotExpandHistoryUnlessAtStartOfLine() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo\n bar\n  baz \n !2\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo", "bar", "baz", "!2"));
  }

  @Test
  public void shouldExpandHistoricalSearch() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo\n bar\n  baz \n!?ba\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo", "bar", "baz", "baz"));
  }

  @Test
  public void shouldExpandLastLine() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo\n bar\n  baz \n!!\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo", "bar", "baz", "baz"));
  }

  @Test
  public void shouldExpandHistoricalLineWithReplacement() throws Exception {
    // Given:
    final JLineReader reader = createReaderForInput("foo\n select col1, col2 from d \n^col2^xyz^\n");

    // When:
    final List<String> commands = readAllLines(reader);

    // Then:
    assertThat(commands, contains("foo", "select col1, col2 from d", "select col1, xyz from d"));
  }

  @SuppressWarnings("InfiniteLoopStatement")
  private List<String> readAllLines(final JLineReader reader) throws IOException {
    final List<String> commands = new ArrayList<>();
    try {
      while (true) {
         String line = reader.readLine();
         commands.add(line.trim());
      }
    } catch (EndOfFileException e) {
      // this indicates end of input in JLine
    }
    return commands;
  }

  private List<String> getHistory(final JLineReader reader) {
    final List<String> commands = new ArrayList<>();
    reader.getHistory().forEach(entry -> commands.add(entry.line()));
    return commands;
  }

  private JLineReader createReaderForInput(final String input) throws IOException {
    final InputStream inputStream =
        new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    final OutputStream outputStream = new ByteArrayOutputStream(512);
    final Terminal terminal = new DumbTerminal(inputStream, outputStream);
    File tempHistoryFile = tempFolder.newFile("ksql-history.txt");
    final Path historyFilePath = Paths.get(tempHistoryFile.getAbsolutePath());
    return new JLineReader(terminal, historyFilePath);
  }
}
