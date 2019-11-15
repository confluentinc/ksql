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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.ExpressionParser;
import io.confluent.ksql.schema.ksql.ColumnRef;
import java.io.IOException;

class ColumnRefDeserializer extends JsonDeserializer<ColumnRef> {
  @Override
  public ColumnRef deserialize(final JsonParser parser, final DeserializationContext ctx)
      throws IOException {
    final Expression expression
        = ExpressionParser.parseExpression(parser.readValueAs(String.class));
    if (expression instanceof ColumnReferenceExp) {
      return ((ColumnReferenceExp) expression).getReference();
    }
    throw new IllegalArgumentException("Passed JSON is not a column reference: " + expression);
  }
}
