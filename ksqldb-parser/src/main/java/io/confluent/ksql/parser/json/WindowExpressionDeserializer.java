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
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.ksql.execution.windows.HoppingWindowExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.parser.ExpressionParser;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;

class WindowExpressionDeserializer<T extends KsqlWindowExpression> extends JsonDeserializer<T> {
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  @SuppressWarnings("unchecked")
  public T deserialize(final JsonParser parser, final DeserializationContext ctx)
      throws IOException {
    final TreeNode root = parser.readValueAsTree();

    if (root instanceof TextNode) {
      // parse old format
      return (T) ExpressionParser.parseWindowExpression(((TextNode) root).textValue());
    } else {
      // parse new format

      final String windowType = ((ObjectNode) root).remove("windowType").textValue();

      switch (windowType) {
        case "TUMBLING":
          return (T) mapper.convertValue(root, TumblingWindowExpression.class);
        case "HOPPING":
          return (T) mapper.convertValue(root, HoppingWindowExpression.class);
        case "SESSION":
          return (T) mapper.convertValue(root, SessionWindowExpression.class);
        default:
          throw new KsqlException("Unknown window type: " + windowType);
      }
    }
  }
}
