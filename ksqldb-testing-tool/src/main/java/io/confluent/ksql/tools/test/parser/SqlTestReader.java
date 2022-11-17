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

package io.confluent.ksql.tools.test.parser;

import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.CaseInsensitiveStream;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.parser.SqlBaseLexer;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.TestStatementContext;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.util.ParserUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

/**
 * The {@code SqlTestReader} is an iterator over a test case that will return
 * one test statement to execute at a time.
 *
 * <p>Since some test directives are not part of the ksqlDB grammar and are
 * handled via non-default ANTLR channels, this parser has a more complex
 * iteration. Specifically, the reader uses the {@code SqlBaseParser} to read
 * statements, but checks to see if any directives were skipped in the underlying
 * hidden channels.</p>
 */
public final class SqlTestReader implements Iterator<TestStatement> {

  private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
    @Override
    public void syntaxError(
        final Recognizer<?, ?> recognizer,
        final Object offendingSymbol,
        final int line,
        final int charPositionInLine,
        final String message,
        final RecognitionException e
    ) {
      throw new ParsingException(message, line, charPositionInLine);
    }
  };

  private final SqlBaseParser parser;
  private final CommonTokenStream tks;

  /* used to evaluate RunScript statements one at a time*/
  private final Deque<ParsedStatement> cachedRunScript = new LinkedList<>();

  /* indicates the latest index in tks that has been scanned for directives */
  private int directiveIdx = 0;

  /* whether or not a statement has been read, but not yet returned */
  private boolean cachedStatement = false;
  private TestStatementContext testStatement;

  public static SqlTestReader of(
      final String test
  ) {
    return new SqlTestReader(CharStreams.fromString(test));
  }

  /**
   * @param file the test file
   */
  public static SqlTestReader of(
      final Path file
  ) throws IOException {
    final CharStream chars = CharStreams.fromPath(file);
    return new SqlTestReader(chars);
  }

  private SqlTestReader(final CharStream chars) {
    Objects.requireNonNull(chars, "chars");

    final SqlBaseLexer lexer = new SqlBaseLexer(new CaseInsensitiveStream(chars));
    lexer.removeErrorListeners();
    lexer.addErrorListener(ERROR_LISTENER);

    tks = new CommonTokenStream(lexer);

    parser = new SqlBaseParser(tks);
    parser.removeErrorListeners();
    parser.addErrorListener(ERROR_LISTENER);
  }

  @Override
  public boolean hasNext() {
    return cachedStatement
        || !cachedRunScript.isEmpty()
        || !parser.isMatchedEOF()
        || hasMoreDirectives();
  }

  private boolean hasMoreDirectives() {
    for (int i = 0; directiveIdx + i < tks.size(); i++) {
      if (tks.get(directiveIdx + i).getChannel() == SqlBaseLexer.DIRECTIVES) {
        return true;
      }
    }
    return false;
  }

  @Override
  public TestStatement next() {
    if (!cachedRunScript.isEmpty()) {
      return TestStatement.of(cachedRunScript.removeFirst());
    }

    // because the parser skips over non-default (SqlBaseLexer.DIRECTIVES) channels,
    // we need to first get the next test statement and then check if we "skipped"
    // over any comments that contain directives. if we find any directives, we
    // cache the statement that the parser read and hold off on returning it
    if (!cachedStatement && !parser.isMatchedEOF()) {
      testStatement = parser.testStatement(); // advances the token stream
      cachedStatement = true;
    }

    // if there's no cachedStatement at this point, then all that's left is the directives
    // so we can just use EOF (tks.size()) as our stopping point
    final int currIdx = cachedStatement ? testStatement.getStart().getTokenIndex() : tks.size();
    while (directiveIdx < currIdx) {
      final Token tok = tks.get(directiveIdx++);
      if (tok.getChannel() == SqlBaseLexer.DIRECTIVES) {
        return TestStatement.of(DirectiveParser.parse(tok));
      }
    }

    cachedStatement = false;
    if (testStatement.singleStatement() != null) {
      return TestStatement.of(
          DefaultKsqlParser.parsedStatement(testStatement.singleStatement())
      );
    } else if (testStatement.runScript() != null) {
      return handleRunScript();
    } else if (testStatement.assertStatement() != null) {
      return TestStatement.of(
          new AstBuilder(TypeRegistry.EMPTY)
              .buildAssertStatement(testStatement.assertStatement())
      );
    }

    throw new IllegalStateException("Unexpected parse tree for statement " + testStatement);
  }

  private TestStatement handleRunScript() {
    final String script = ParserUtil.unquote(testStatement.runScript().STRING().getText(), "'");
    final List<String> lines;
    try {
      if (getClass().getResource(script) != null) {
        lines = Files.readAllLines(Paths.get(getClass().getResource(script).getPath()));
      } else {
        lines = Files.readAllLines(Paths.get(script));
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not read " + script, e);
    }

    try {
      cachedRunScript.addAll(new DefaultKsqlParser().parse(String.join("\n", lines)));
    } catch (final ParseFailedException e) {
      throw new ParseFailedException(
          "Failed to parse contents of RUN SCRIPT", "RUN SCRIPT '" + script + "';", e);
    }

    if (cachedRunScript.isEmpty()) {
      throw new IllegalArgumentException("Empty run script: " + script);
    }

    return TestStatement.of(cachedRunScript.removeFirst());
  }

}
