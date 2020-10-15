/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.parser.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.CaseInsensitiveStream;
import io.confluent.ksql.parser.SqlBaseLexer;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.TableElementContext;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.ParserUtil;
import java.io.IOException;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class ColumnDeserialization extends JsonDeserializer<Column> {

  public ColumnDeserialization() {
  }

  @Override
  public Column deserialize(
      final JsonParser p,
      final DeserializationContext ctxt
  ) throws IOException {
    final String text = p.readValueAs(String.class);
    final SqlBaseLexer lexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString(text))
    );
    final CommonTokenStream tokStream = new CommonTokenStream(lexer);
    final SqlBaseParser parser = new SqlBaseParser(tokStream);
    final TableElementContext context = parser.tableElement();

    return Column.of(
        ColumnName.of(ParserUtil.getIdentifierText(context.identifier())),
        SqlTypeParser.create(TypeRegistry.EMPTY).getType(context.type()).getSqlType(),
        context.KEY() == null ? Namespace.VALUE : Namespace.KEY,
        0
    );
  }
}
