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

import com.google.common.collect.Iterables;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Helper for dealing with Struct keys.
 */
public final class StructKeyUtil {

  private StructKeyUtil() {
  }

  public static KeyBuilder keyBuilder(final LogicalSchema schema) {
    final List<Column> keyCols = schema.key();
    if (keyCols.size() != 1) {
      throw new UnsupportedOperationException("Only single keys supported");
    }

    final Column keyCol = keyCols.get(0);
    return keyBuilder(keyCol.name(), keyCol.type());
  }

  public static KeyBuilder keyBuilder(final ColumnName name, final SqlType type) {
    final Schema connectSchema = SchemaConverters.sqlToConnectConverter().toConnectSchema(type);

    return new KeyBuilder(SchemaBuilder
        .struct()
        .field(name.text(), connectSchema)
        .build()
    );
  }

  @SuppressWarnings("unchecked")
  public static List<?> asList(final Object key) {
    final Optional<Windowed<Object>> windowed = key instanceof Windowed
        ? Optional.of((Windowed<Object>) key)
        : Optional.empty();

    final Object naturalKey = windowed
        .map(Windowed::key)
        .orElse(key);

    if (naturalKey != null && !(naturalKey instanceof Struct)) {
      throw new IllegalArgumentException("None struct key: " + key);
    }

    final Optional<Struct> structKey = Optional.ofNullable((Struct) naturalKey);

    final List<Object> data = new ArrayList<>(3);

    structKey.ifPresent(k ->
        k.schema().fields().stream()
            .map(k::get)
            .forEach(data::add)
    );

    windowed
        .map(Windowed::window)
        .ifPresent(wnd -> {
          data.add(wnd.start());
          data.add(wnd.end());
        });

    return data;
  }

  public static final class KeyBuilder {

    private final Schema keySchema;
    private final org.apache.kafka.connect.data.Field keyField;

    private KeyBuilder(final Schema keySchema) {
      this.keySchema = Objects.requireNonNull(keySchema, "keySchema");
      this.keyField = Iterables.getOnlyElement(keySchema.fields());
    }

    public Struct build(final Object keyValue) {
      final Struct keyStruct = new Struct(keySchema);
      keyStruct.put(keyField, keyValue);
      return keyStruct;
    }
  }
}
