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
    final String input = "  show streams\n";
    final JLineReader reader = createReaderForInput(input);

    reader.readLine();

    final List<String> commands = new ArrayList<>();
    reader.getHistory().forEach(entry -> commands.add(entry.line()));

    assertThat(commands, contains(input.trim()));
  }

  @Test
  public void shouldExpandHistoricalLine() throws Exception {
    final JLineReader reader = createReaderForInput("foo\n bar\n  baz \n!2\n");
    final List<String> commands = new ArrayList<>();
    try {
      while (true) {
         String line = reader.readLine();
         commands.add(line.trim());
      }
    } catch (EndOfFileException e) {
      // this indicates end of input in JLine
    }
    assertThat(commands, hasSize(4));
    assertThat(commands, contains("foo", "bar", "baz", "bar"));
  }

  @Test
  public void shouldExpandRelativeLine() throws Exception {
    final JLineReader reader = createReaderForInput("foo\n bar\n  baz \n!-3\n");
    final List<String> commands = new ArrayList<>();
    try {
      while (true) {
         String line = reader.readLine();
         commands.add(line.trim());
      }
    } catch (EndOfFileException e) {
      // this indicates end of input in JLine
    }
    assertThat(commands, hasSize(4));
    assertThat(commands, contains("foo", "bar", "baz", "foo"));
  }

  @Test
  public void shouldNotExpandHistoryUnlessAtStartOfLine() throws Exception {
    final JLineReader reader = createReaderForInput("foo\n bar\n  baz \n !2\n");
    final List<String> commands = new ArrayList<>();
    try {
      while (true) {
         String line = reader.readLine();
         commands.add(line.trim());
      }
    } catch (EndOfFileException e) {
      // this indicates end of input in JLine
    }
    assertThat(commands, hasSize(4));
    assertThat(commands, contains("foo", "bar", "baz", "!2"));
  }

  @Test
  public void shouldExpandHistoricalSearch() throws Exception {
    final JLineReader reader = createReaderForInput("foo\n bar\n  baz \n!?ba\n");
    final List<String> commands = new ArrayList<>();
    try {
      while (true) {
         String line = reader.readLine();
         commands.add(line.trim());
      }
    } catch (EndOfFileException e) {
      // this indicates end of input in JLine
    }
    assertThat(commands, hasSize(4));
    assertThat(commands, contains("foo", "bar", "baz", "baz"));
  }

  @Test
  public void shouldExpandLastLine() throws Exception {
    final JLineReader reader = createReaderForInput("foo\n bar\n  baz \n!!\n");
    final List<String> commands = new ArrayList<>();
    try {
      while (true) {
         String line = reader.readLine();
         commands.add(line.trim());
      }
    } catch (EndOfFileException e) {
      // this indicates end of input in JLine
    }
    assertThat(commands, hasSize(4));
    assertThat(commands, contains("foo", "bar", "baz", "baz"));
  }

  @Test
  public void shouldExpandHistoricalLineWithReplacement() throws Exception {
    final JLineReader reader = createReaderForInput("foo\n select col1, col2 from d \n^col2^xyz^\n");
    final List<String> commands = new ArrayList<>();
    try {
      while (true) {
         String line = reader.readLine();
         commands.add(line.trim());
      }
    } catch (EndOfFileException e) {
      // this indicates end of input in JLine
    }
    assertThat(commands, hasSize(3));
    assertThat(commands, contains("foo", "select col1, col2 from d", "select col1, xyz from d"));
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
