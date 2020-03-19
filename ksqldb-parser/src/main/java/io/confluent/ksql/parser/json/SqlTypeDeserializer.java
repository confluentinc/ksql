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
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.io.IOException;

public class SqlTypeDeserializer extends JsonDeserializer<SqlType> {

  public SqlTypeDeserializer() {
  }

  @Override
  public SqlType deserialize(
      final JsonParser p,
      final DeserializationContext ctxt
  ) throws IOException {
    final String text = p.readValueAs(String.class);
    return SqlTypeParser.create(TypeRegistry.EMPTY).parse(text).getSqlType();
  }
}
