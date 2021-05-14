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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.confluent.ksql.execution.expression.formatter.ExpressionFormatter;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.IdentifierUtil;
import java.io.IOException;

class ExpressionSerializer extends JsonSerializer<Expression> {
  @Override
  public void serialize(
      final Expression expression,
      final JsonGenerator jsonGenerator,
      final SerializerProvider serializerProvider
  ) throws IOException {
    jsonGenerator.writeObject(
        ExpressionFormatter.formatExpression(
            expression,
            FormatOptions.of(IdentifierUtil::needsQuotes))
    );
  }
}
