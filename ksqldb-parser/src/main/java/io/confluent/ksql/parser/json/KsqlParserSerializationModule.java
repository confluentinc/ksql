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

import com.fasterxml.jackson.databind.module.SimpleModule;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.schema.ksql.Column;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlParserSerializationModule extends SimpleModule {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  public KsqlParserSerializationModule() {
    super();
    addSerializer(Expression.class, new ExpressionSerializer());
    addDeserializer(Expression.class, new ExpressionDeserializer<>());
    addDeserializer(FunctionCall.class, new ExpressionDeserializer<>());
    addSerializer(SelectExpression.class, new SelectExpressionSerializer());
    addDeserializer(SelectExpression.class, new SelectExpressionDeserializer());
    addSerializer(KsqlWindowExpression.class, new KsqlWindowExpressionSerializer());
    addDeserializer(KsqlWindowExpression.class, new WindowExpressionDeserializer<>());
    addSerializer(Column.class, new ColumnSerializer());
    addDeserializer(Column.class, new ColumnDeserializor());
  }
}
