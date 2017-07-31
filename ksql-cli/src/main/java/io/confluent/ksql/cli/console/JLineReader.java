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
import org.jline.terminal.Terminal;

import java.io.IOException;

public class JLineReader {

  private static final String DEFAULT_PROMPT = "ksql> ";

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

    this.prompt = DEFAULT_PROMPT;
  }

  public Iterable<? extends History.Entry> getHistory() {
    return lineReader.getHistory();
  }

  public String readLine() throws IOException {
    return lineReader.readLine(prompt);
  }

}
