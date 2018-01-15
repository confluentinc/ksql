/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.serde.delimited;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;

import java.nio.charset.StandardCharsets;
import java.util.Map;


public class KsqlDelimitedSerializer implements Serializer<GenericRow> {

  private final Schema schema;

  public KsqlDelimitedSerializer(Schema schema) {
    this.schema = schema;
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public byte[] serialize(final String topic, final GenericRow genericRow) {
    if (genericRow == null) {
      return null;
    }

    try {
      StringBuilder recordString = new StringBuilder();
      for (int i = 0; i < genericRow.getColumns().size(); i++) {
        if (i != 0) {
          recordString.append(",");
        }
        if (genericRow.getColumns().get(i) == null) {
          recordString.append("null");
        } else if (schema.fields().get(i).schema().type() == Schema.Type.STRING) {
          recordString.append("'" + genericRow.getColumns().get(i).toString() + "'");
        } else {
          recordString.append(genericRow.getColumns().get(i).toString());
        }

      }
      return recordString.toString().getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new KsqlException(e.getMessage(), e);
    }


  }

  @Override
  public void close() {

  }
}
