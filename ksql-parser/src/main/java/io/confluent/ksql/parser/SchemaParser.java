/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.parser;

import static io.confluent.ksql.schema.ksql.TypeContextUtil.getType;
import static io.confluent.ksql.util.ParserUtil.getLocation;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.ParserUtil;
import java.util.List;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public final class SchemaParser {

  private SchemaParser() { }

  public static List<TableElement> parse(final String schema) {
    if (schema.isEmpty()) {
      return ImmutableList.of();
    }

    final SqlBaseLexer lexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString("(" + schema + ")")));
    final CommonTokenStream tokStream = new CommonTokenStream(lexer);
    final SqlBaseParser parser = new SqlBaseParser(tokStream);

    parser.addErrorListener(new BaseErrorListener() {
      @Override
      public void syntaxError(
          final Recognizer<?, ?> recognizer,
          final Object offendingSymbol,
          final int line,
          final int charPositionInLine,
          final String msg,
          final RecognitionException e) {
        throw new KsqlException(
            String.format("Error parsing schema \"%s\" at %d:%d: %s",
                schema,
                line,
                charPositionInLine,
                msg),
            e);
      }
    });

    return parser.tableElements().tableElement()
        .stream()
        .map(ctx -> new TableElement(
            getLocation(ctx),
            ParserUtil.getIdentifierText(ctx.identifier()),
            getType(ctx.type())))
        .collect(Collectors.toList());
  }
}
