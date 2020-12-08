package io.confluent.ksql.rest.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.util.KsqlException;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a single file of commands issued to this node.
 */
public class LocalCommandsFile  implements Closeable {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final byte[] NEW_LINE_SEPARATOR_BYTES =
      "\n".getBytes(StandardCharsets.UTF_8);


  private final File file;
  private final FileOutputStream writer;

  private LocalCommandsFile(final File file, final boolean write) {
    this.file = Objects.requireNonNull(file, "file");

    if (write) {
      this.writer = createWriter(file);
    } else {
      this.writer = null;
    }
  }

  public static LocalCommandsFile createReadonly(final File file) {
    return new LocalCommandsFile(file, false);
  }

  public static LocalCommandsFile createWriteable(final File file) {
    return new LocalCommandsFile(file, true);
  }

  public void write(final LocalCommand localCommand) throws IOException {
    if (writer == null) {
      throw new IOException("Write permission denied.");
    }

    byte[] bytes = MAPPER.writeValueAsBytes(localCommand);
    writer.write(bytes);
    writer.write(NEW_LINE_SEPARATOR_BYTES);
    writer.flush();
  }

  public List<LocalCommand> readRecords() throws IOException {
    final List<LocalCommand> localCommands = new ArrayList<>();
    for (final String line : Files.readAllLines(file.toPath(), StandardCharsets.UTF_8)) {
      LocalCommand localCommand = MAPPER.readValue(line, LocalCommand.class);
      localCommands.add(localCommand);
    }
    return localCommands;
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  public File getFile() {
    return file;
  }

  private static FileOutputStream createWriter(final File file) {
    try {
      return new FileOutputStream(file, true);
    } catch (final FileNotFoundException e) {
      throw new KsqlException(
          String.format("Failed to create/open replay file: %s", file.getAbsolutePath()), e);
    }
  }
}
