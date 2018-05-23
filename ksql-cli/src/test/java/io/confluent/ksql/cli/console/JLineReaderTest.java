package io.confluent.ksql.cli.console;

import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.Before;
import org.junit.Test;

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

import static org.testng.Assert.assertEquals;

public class JLineReaderTest {

  @Test
  public void shouldSaveCommandsWithLeadingSpacesToHistory() throws IOException  {
    final String input = "  show streams\n";
    final JLineReader reader = createReaderForInput(input);

    reader.readLine();

    final List<String> commands = new ArrayList<>();
    reader.getHistory().forEach(entry -> commands.add(entry.line()));

    assertEquals(1, commands.size());
    assertEquals(input.trim(), commands.get(0));
  }

  private JLineReader createReaderForInput(String input) throws IOException {
    InputStream inputStream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    OutputStream outputStream = new ByteArrayOutputStream(512);
    Terminal terminal = new DumbTerminal(inputStream, outputStream);
    Path historyFilePath = Files.createTempFile("ksql-history", "txt").toAbsolutePath();
    return new JLineReader(terminal, historyFilePath);
  }
}
