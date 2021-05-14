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

package io.confluent.ksql.parser.json;

import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.Operator;
import java.util.Optional;

public class SelectExpressionTestCase {
  static final SelectExpression SELECT_EXPRESSION = SelectExpression.of(
      ColumnName.of("FOO"),
      new ArithmeticBinaryExpression(
          Operator.ADD,
          new IntegerLiteral(1),
          new IntegerLiteral(2)
      )
  );
  static final String SELECT_EXPRESSION_TXT = "\"(1 + 2) AS FOO\"";
  static final SelectExpression SELECT_EXPRESSION_NEEDS_QUOTES = SelectExpression.of(
      ColumnName.of("TEST"),
      new DereferenceExpression(
          Optional.empty(),
          new UnqualifiedColumnReferenceExp(ColumnName.of("STREAM")),
          "foo"
      )
  );
  static final String SELECT_EXPRESSION_NEEDS_QUOTES_TXT =
      "\"`STREAM`->`foo` AS TEST\"";
  static final SelectExpression SELECT_EXPRESSION_NAME_NEEDS_QUOTES = SelectExpression.of(
      ColumnName.of("STREAM"),
      new ArithmeticBinaryExpression(
          Operator.ADD,
          new IntegerLiteral(1),
          new IntegerLiteral(2)
      )
  );
  static final String SELECT_EXPRESSION_NAME_NEEDS_QUOTES_TXT = "\"(1 + 2) AS `STREAM`\"";
}
