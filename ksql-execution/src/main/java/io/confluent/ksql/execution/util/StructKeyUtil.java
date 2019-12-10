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

package io.confluent.ksql.execution.util;

import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Helper for dealing with Struct keys.
 */
public final class StructKeyUtil {

  private static final Schema ROWKEY_STRUCT_SCHEMA = SchemaBuilder
      .struct()
      .field(SchemaUtil.ROWKEY_NAME.name(), Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  private static final org.apache.kafka.connect.data.Field ROWKEY_FIELD =
      ROWKEY_STRUCT_SCHEMA.fields().get(0);

  private StructKeyUtil() {
  }

  public static Struct asStructKey(String rowKey) {
    Struct keyStruct = new Struct(ROWKEY_STRUCT_SCHEMA);
    keyStruct.put(ROWKEY_FIELD, rowKey);
    return keyStruct;
  }

  public static KeyBuilder keyBuilder(final LogicalSchema schema) {
    final List<Column> keyCols = schema.key();
    if (keyCols.size() != 1) {
      throw new UnsupportedOperationException("Only single keys supported");
    }

    final SqlType sqlType = keyCols.get(0).type();
    return keyBuilder(sqlType);
  }

  public static KeyBuilder keyBuilder(final SqlType sqlType) {
    final Schema connectSchema = SchemaConverters.sqlToConnectConverter().toConnectSchema(sqlType);

    return new KeyBuilder(SchemaBuilder
        .struct()
        .field(SchemaUtil.ROWKEY_NAME.name(), connectSchema)
        .build()
    );
  }

  public static final class KeyBuilder {

    private final Schema keySchema;
    private final org.apache.kafka.connect.data.Field keyField;

    private KeyBuilder(final Schema keySchema) {
      this.keySchema = Objects.requireNonNull(keySchema, "keySchema");
      this.keyField = keySchema.field(SchemaUtil.ROWKEY_NAME.name());
    }

    public Struct build(final Object rowKey) {
      final Struct keyStruct = new Struct(keySchema);
      keyStruct.put(keyField, rowKey);
      return keyStruct;
    }
  }
}
