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

import static io.confluent.ksql.util.ParserUtil.getLocation;
import static java.util.Objects.requireNonNull;

import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
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

  private final TypeRegistry typeRegistry;

  public SchemaParser(final TypeRegistry typeRegistry) {
    this.typeRegistry = requireNonNull(typeRegistry, "typeRegistry");
  }

  public static TableElements parse(final String schema, final TypeRegistry typeRegistry) {
    return new SchemaParser(typeRegistry).parse(schema);
  }

  public TableElements parse(final String schema) {
    if (schema.trim().isEmpty()) {
      return TableElements.of();
    }

    final SqlBaseLexer lexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString("(" + schema + ")")));
    final CommonTokenStream tokStream = new CommonTokenStream(lexer);
    final SqlBaseParser parser = new SqlBaseParser(tokStream);

    final BaseErrorListener errorListener = new BaseErrorListener() {
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
    };

    lexer.removeErrorListeners();
    lexer.addErrorListener(errorListener);

    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);

    final SqlTypeParser typeParser = SqlTypeParser.create(typeRegistry);

    final List<TableElement> elements = parser.tableElements().tableElement()
        .stream()
        .map(ctx -> new TableElement(
            getLocation(ctx),
            ColumnName.of(ParserUtil.getIdentifierText(ctx.identifier())),
            typeParser.getType(ctx.type()),
            ParserUtil.getColumnConstraints(ctx.columnConstraints())
        ))
        .collect(Collectors.toList());

    return TableElements.of(elements);
  }
}
