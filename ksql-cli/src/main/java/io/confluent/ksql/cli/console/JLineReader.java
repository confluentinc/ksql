/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli.console;

import org.jline.reader.Expander;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.DefaultExpander;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;

import java.io.IOException;
import java.nio.file.Paths;

public class JLineReader implements io.confluent.ksql.cli.console.LineReader {

  private static final String DEFAULT_PROMPT = "ksql> ";

  private final DefaultHistory history;

  private final LineReader lineReader;
  private final String prompt;

  // Have to enable event expansion or multi-line parsing won't work, so a quick 'n dirty workaround
  // will have to do to prevent strings like !! from being expanded by the line reader
  private static class NoOpExpander extends DefaultExpander {
    @Override
    public String expandHistory(History history, String line) {
      return line;
    }
  }

  public JLineReader(Terminal terminal) {
    Expander expander = new NoOpExpander();

    // The combination of parser/expander here allow for multiple-line commands connected by '\\'
    DefaultParser parser = new DefaultParser();
    parser.setEofOnEscapedNewLine(true);
    parser.setQuoteChars(new char[0]);
    parser.setEscapeChars(new char[] {'\\'});

    // TODO: specify a completer to use here via a call to LineReaderBuilder.completer()
    this.lineReader = LineReaderBuilder.builder()
        .appName("KSQL")
        .expander(expander)
        .parser(parser)
        .terminal(terminal)
        .build();

    this.lineReader.setOpt(LineReader.Option.HISTORY_IGNORE_DUPS);
    this.lineReader.setOpt(LineReader.Option.HISTORY_IGNORE_SPACE);

    this.lineReader.setVariable(LineReader.HISTORY_FILE, Paths.get( System.getProperty("history-file",  System.getProperty("user.home") + "/.ksql-history")));
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
    String line = lineReader.readLine(prompt);
    history.add(line);
    history.save();
    return line;
  }

}
