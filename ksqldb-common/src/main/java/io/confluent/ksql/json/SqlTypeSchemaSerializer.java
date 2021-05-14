/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.io.IOException;

public class SqlTypeSchemaSerializer extends JsonSerializer<SqlType> {

  @Override
  public void serialize(
      final SqlType value,
      final JsonGenerator gen,
      final SerializerProvider serializers
  ) throws IOException {
    gen.writeString(value.toString(FormatOptions.none()));
  }
}
