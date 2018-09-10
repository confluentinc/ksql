/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.serde.avro;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import java.util.Collections;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;

public class AvroSchemaTranslator extends ConnectSchemaTranslator {
  public static Schema toKsqlSchema(final String avroSchemaString) {
    final org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(avroSchemaString);
    final AvroData avroData = new AvroData(new AvroDataConfig(Collections.emptyMap()));
    return new AvroSchemaTranslator().toKsqlSchema(avroData.toConnectSchema(avroSchema));
  }

  @Override
  protected Schema toKsqlFieldSchema(final Schema connectSchema) {
    if (connectSchema.type().equals(Schema.Type.STRUCT)
        && Objects.equals(connectSchema.name(), AvroData.AVRO_TYPE_UNION)) {
      throw new UnsupportedTypeException("Union type not supported");
    }
    return super.toKsqlFieldSchema(connectSchema);
  }
}
