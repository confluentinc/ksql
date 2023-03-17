/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.parser;

import java.util.function.Function;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

final class GrammarParseUtil {
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
      throw new ParsingException(
          message,
          line,
          charPositionInLine
      );
    }
  };

  private GrammarParseUtil() {
  }

  static <T extends ParserRuleContext> T getParseTree(
      final String text,
      final Function<SqlBaseParser, T> parseFunction) {
    final SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString(text)));
    final CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);
    final SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

    sqlBaseLexer.removeErrorListeners();
    sqlBaseLexer.addErrorListener(ERROR_LISTENER);

    sqlBaseParser.removeErrorListeners();
    sqlBaseParser.addErrorListener(ERROR_LISTENER);

    try {
      // first, try parsing w/ potentially faster SLL mode
      sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
      return castContext(parseFunction.apply(sqlBaseParser));
    } catch (final ParseCancellationException ex) {
      // if we fail, parse with LL mode
      tokenStream.seek(0); // rewind input stream
      sqlBaseParser.reset();

      sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.LL);
      return castContext(parseFunction.apply(sqlBaseParser));
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends ParserRuleContext> T castContext(final ParserRuleContext ctx) {
    return (T) ctx;
  }
}
