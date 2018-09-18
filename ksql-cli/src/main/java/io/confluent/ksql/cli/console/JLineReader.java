/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.cli.console;

import io.confluent.ksql.util.CliUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.jline.reader.Expander;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.DefaultExpander;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JLineReader implements io.confluent.ksql.cli.console.LineReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(JLineReader.class);

  private static final String DEFAULT_PROMPT = "ksql> ";

  private final DefaultHistory history;

  private final LineReader lineReader;
  private final String prompt;

  /**
   * Override the default JLine 'expander' behavior so that history-referencing expressions such
   * as '!42' or '!!' will only be processed and replaced if they occur at the beginning of an
   * input line, even if the input line spans multiple terminal lines using the '\' separator.
   */
  private static class KsqlExpander extends DefaultExpander {

    @Override
    public String expandHistory(final History history, final String line) {
      if (line.startsWith("!") || line.startsWith("^")) {
        return super.expandHistory(history, line);
      } else {
        return line;
      }
    }

  }

  JLineReader(final Terminal terminal, final Path historyFilePath) {
    // The combination of parser/expander here allow for multiple-line commands connected by '\\'
    final DefaultParser parser = new DefaultParser();
    parser.setEofOnEscapedNewLine(true);
    parser.setQuoteChars(new char[0]);
    parser.setEscapeChars(new char[]{'\\'});

    final Expander expander = new KsqlExpander();
    // TODO: specify a completer to use here via a call to LineReaderBuilder.completer()
    this.lineReader = LineReaderBuilder.builder()
        .appName("KSQL")
        .variable(LineReader.SECONDARY_PROMPT_PATTERN, ">")
        .option(LineReader.Option.HISTORY_IGNORE_DUPS, true)
        .option(LineReader.Option.HISTORY_IGNORE_SPACE, false)
        .expander(expander)
        .parser(new TrimmingParser(parser))
        .terminal(terminal)
        .build();

    if (Files.exists(historyFilePath) || CliUtils.createFile(historyFilePath)) {
      this.lineReader.setVariable(LineReader.HISTORY_FILE, historyFilePath);
      LOGGER.info("Command history saved at: " + historyFilePath);
    } else {
      terminal.writer().println(String.format(
          "WARNING: Unable to create command history file '%s', command history will not be saved.",
          historyFilePath
      ));
    }

    this.lineReader.unsetOpt(LineReader.Option.HISTORY_INCREMENTAL);
    this.history = new DefaultHistory(this.lineReader);

    this.prompt = DEFAULT_PROMPT;
  }

  @Override
  public Iterable<? extends History.Entry> getHistory() {
    return lineReader.getHistory();
  }

  @Override
  public String readLine() throws IOException {
    final String line = lineReader.readLine(prompt);
    history.add(line);
    history.save();
    return line;
  }
}
