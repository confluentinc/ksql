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

package io.confluent.ksql.parser.json;

import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.ColumnRef;
import java.util.Optional;

public class ExpressionTestCase {
  static final Expression EXPRESSION = new ArithmeticBinaryExpression(
      Operator.ADD, new IntegerLiteral(1), new IntegerLiteral(2)
  );
  static final String EXPRESSION_TXT = "\"(1 + 2)\"";

  static final Expression EXPRESSION_NEEDS_QUOTES = new DereferenceExpression(
      Optional.empty(),
      new ColumnReferenceExp(ColumnRef.of(SourceName.of("FOO"), ColumnName.of("STREAM"))),
      "bar"
  );
  static final String EXPRESSION_NEEDS_QUOTES_TXT = "\"FOO.`STREAM`->`bar`\"";
}
