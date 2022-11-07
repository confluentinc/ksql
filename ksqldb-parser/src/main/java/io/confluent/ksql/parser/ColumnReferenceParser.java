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

import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.SqlBaseParser.ColumnReferenceContext;
import io.confluent.ksql.parser.SqlBaseParser.PrimaryExpressionContext;
import io.confluent.ksql.parser.SqlBaseParser.QualifiedColumnReferenceContext;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.util.ParserUtil;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public final class ColumnReferenceParser {

  private ColumnReferenceParser() {
  }

  /**
   * Parses text that represents a column (such as values referencing keys/timestamps
   * in the WITH clause). This will use the same semantics as the {@link AstBuilder},
   * namely it will uppercase anything that is unquoted, and remove quotes but leave
   * the case sensitivity for anything that is quoted. If the text contains an unquoted
   * ".", it is considered a delimiter between the source and the column name.
   *
   * @param text the text to parse
   * @return a {@code ColumnRef}
   * @throws ParseFailedException if the parse fails (this does not match the pattern
   *                              for a column reference
   */
  public static ColumnName parse(final String text) {
    final SqlBaseLexer lexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString(text))
    );
    final CommonTokenStream tokStream = new CommonTokenStream(lexer);
    final SqlBaseParser parser = new SqlBaseParser(tokStream);

    final PrimaryExpressionContext primaryExpression = parser.primaryExpression();
    if (primaryExpression instanceof ColumnReferenceContext) {
      return resolve((ColumnReferenceContext) primaryExpression).getColumnName();
    }
    if (primaryExpression instanceof QualifiedColumnReferenceContext) {
      return resolve((QualifiedColumnReferenceContext) primaryExpression).getColumnName();
    }

    throw new ParseFailedException("Cannot parse text that is not column reference.", text);
  }

  static UnqualifiedColumnReferenceExp resolve(final ColumnReferenceContext context) {
    return new UnqualifiedColumnReferenceExp(
        getLocation(context),
        ColumnName.of(ParserUtil.getIdentifierText(context.identifier()))
    );
  }

  static QualifiedColumnReferenceExp resolve(final QualifiedColumnReferenceContext context) {
    return new QualifiedColumnReferenceExp(
        getLocation(context),
        SourceName.of(ParserUtil.getIdentifierText(context.identifier(0))),
        ColumnName.of(ParserUtil.getIdentifierText(context.identifier(1)))
    );
  }
}
