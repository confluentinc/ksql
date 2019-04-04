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

package io.confluent.ksql.schema.ksql;

import static io.confluent.ksql.schema.ksql.TypeContextUtil.getType;

import io.confluent.ksql.parser.CaseInsensitiveStream;
import io.confluent.ksql.parser.SqlBaseLexer;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.TypeContext;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.kafka.connect.data.Schema;

public class DefaultSchemaParser implements SchemaParser {

  @Override
  public Schema parse(final String schema) {
    final SqlBaseParser.TypeContext typeContext = getTypeContext(schema);
    return LogicalSchemas.fromSqlTypeConverter().fromSqlType(getType(typeContext));
  }

  private static TypeContext getTypeContext(final String schema) {
    final SqlBaseLexer lexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString(schema)));
    final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    final SqlBaseParser parser = new SqlBaseParser(tokenStream);

    try {
      parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
      return parser.type();
    } catch (final ParseCancellationException ex) {
      tokenStream.seek(0);
      parser.reset();

      parser.getInterpreter().setPredictionMode(PredictionMode.LL);
      return parser.type();
    }
  }

}
