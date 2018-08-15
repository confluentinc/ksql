package io.confluent.ksql.cli.console;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.Test;

public class JLineReaderTest {

  @Test
  public void shouldSaveCommandsWithLeadingSpacesToHistory() throws IOException  {
    final String input = "  show streams\n";
    final JLineReader reader = createReaderForInput(input);

    reader.readLine();

    final List<String> commands = new ArrayList<>();
    reader.getHistory().forEach(entry -> commands.add(entry.line()));

    assertThat(commands, contains(input.trim()));
  }

  private JLineReader createReaderForInput(final String input) throws IOException {
    final InputStream inputStream = new ByteArrayInputStream(
        input.getBytes(StandardCharsets.UTF_8));
    final OutputStream outputStream = new ByteArrayOutputStream(512);
    final Terminal terminal = new DumbTerminal(inputStream, outputStream);
    final Path historyFilePath = Files.createTempFile("ksql-history", "txt");
    return new JLineReader(terminal, historyFilePath);
  }
}
