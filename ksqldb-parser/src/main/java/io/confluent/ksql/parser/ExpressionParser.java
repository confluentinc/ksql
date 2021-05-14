/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.util.ParserUtil;
import org.antlr.v4.runtime.ParserRuleContext;

public final class ExpressionParser {
  private ExpressionParser() {
  }

  public static SelectExpression parseSelectExpression(final String expressionText) {
    final SqlBaseParser.SelectItemContext parseCtx = GrammarParseUtil.getParseTree(
        expressionText,
        SqlBaseParser::selectItem
    );
    if (!(parseCtx instanceof SqlBaseParser.SelectSingleContext)) {
      throw new IllegalArgumentException("Illegal select item type in: " + expressionText);
    }
    final SqlBaseParser.SelectSingleContext selectSingleContext =
        (SqlBaseParser.SelectSingleContext) parseCtx;
    if (selectSingleContext.identifier() == null) {
      throw new IllegalArgumentException("Select item must have identifier in: " + expressionText);
    }
    return SelectExpression.of(
        ColumnName.of(ParserUtil.getIdentifierText(selectSingleContext.identifier())),
        new AstBuilder(TypeRegistry.EMPTY).buildExpression(selectSingleContext.expression())
    );
  }

  public static Expression parseExpression(final String expressionText) {
    final ParserRuleContext parseTree = GrammarParseUtil.getParseTree(
        expressionText,
        SqlBaseParser::singleExpression
    );
    return new AstBuilder(TypeRegistry.EMPTY).buildExpression(parseTree);
  }

  public static KsqlWindowExpression parseWindowExpression(final String expressionText) {
    final ParserRuleContext parseTree = GrammarParseUtil.getParseTree(
        expressionText,
        SqlBaseParser::windowExpression
    );
    final WindowExpression windowExpression =
        new AstBuilder(TypeRegistry.EMPTY).buildWindowExpression(parseTree);
    return windowExpression.getKsqlWindowExpression();
  }
}
