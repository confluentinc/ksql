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

package io.confluent.ksql.structured;

import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Helper for dealing with Struct keys.
 */
final class StructKeyUtil {

  private static final Schema ROWKEY_STRUCT_SCHEMA = SchemaBuilder
      .struct()
      .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  private static final org.apache.kafka.connect.data.Field ROWKEY_FIELD =
      ROWKEY_STRUCT_SCHEMA.fields().get(0);

  static final PersistenceSchema ROWKEY_SERIALIZED_SCHEMA = PersistenceSchema.from(
      (ConnectSchema) ROWKEY_STRUCT_SCHEMA,
      false
  );

  private StructKeyUtil() {
  }

  static Struct asStructKey(final String rowKey) {
    final Struct keyStruct = new Struct(ROWKEY_STRUCT_SCHEMA);
    keyStruct.put(ROWKEY_FIELD, rowKey);
    return keyStruct;
  }
}
