/*
 * Copyright 2018 Confluent Inc.
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
package io.confluent.ksql.util;

import static io.confluent.ksql.GenericRow.genericRow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOptions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class StructuredTypesDataProvider extends TestDataProvider<String> {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("STR"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("LONG"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("DEC"), SqlTypes.decimal(4, 2))
      .valueColumn(ColumnName.of("ARRAY"), SqlTypes.array(SqlTypes.STRING))
      .valueColumn(ColumnName.of("MAP"), SqlTypes.map(SqlTypes.STRING))
      .valueColumn(ColumnName.of("STRUCT"), SqlTypes.struct().field("F1", SqlTypes.INTEGER).build())
      .valueColumn(ColumnName.of("COMPLEX"), SqlTypes.struct()
          .field("DECIMAL", SqlTypes.decimal(2, 1))
          .field("STRUCT", SqlTypes.struct()
              .field("F1", SqlTypes.STRING)
              .field("F2", SqlTypes.INTEGER)
              .build())
          .field("ARRAY_ARRAY", SqlTypes.array(SqlTypes.array(SqlTypes.STRING)))
          .field("ARRAY_STRUCT", SqlTypes.array(SqlTypes.struct().field("F1", SqlTypes.STRING).build()))
          .field("ARRAY_MAP", SqlTypes.array(SqlTypes.map(SqlTypes.INTEGER)))
          .field("MAP_ARRAY", SqlTypes.map(SqlTypes.array(SqlTypes.STRING)))
          .field("MAP_MAP", SqlTypes.map(SqlTypes.map(SqlTypes.INTEGER)))
          .field("MAP_STRUCT", SqlTypes.map(SqlTypes.struct().field("F1", SqlTypes.STRING).build()))
          .build()
      )
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(LOGICAL_SCHEMA, SerdeOptions.of());

  private static final Schema STRUCT_FIELD_SCHEMA = LOGICAL_SCHEMA.valueConnectSchema().field("STRUCT").schema();
  private static final Schema COMPLEX_FIELD_SCHEMA = LOGICAL_SCHEMA.valueConnectSchema().field("COMPLEX").schema();

  private static final Multimap<String, GenericRow> ROWS = ImmutableListMultimap
      .<String, GenericRow>builder()
      .put("FOO", genericRow(1L, new BigDecimal("1.11"), Collections.singletonList("a"), Collections.singletonMap("k1", "v1"), generateStruct(2), generateComplexStruct(0)))
      .put("BAR", genericRow(2L, new BigDecimal("2.22"), Collections.emptyList(), Collections.emptyMap(), generateStruct(3), generateComplexStruct(1)))
      .put("BAZ", genericRow(3L, new BigDecimal("30.33"), Collections.singletonList("b"), Collections.emptyMap(), generateStruct(null), generateComplexStruct(2)))
      .put("BUZZ", genericRow(4L, new BigDecimal("40.44"), ImmutableList.of("c", "d"), Collections.emptyMap(), generateStruct(88), generateComplexStruct(3)))
      // Additional entries for repeated keys
      .put("BAZ", genericRow(5L, new BigDecimal("12"), ImmutableList.of("e"), ImmutableMap.of("k1", "v1", "k2", "v2"), generateStruct(0), generateComplexStruct(4)))
      .put("BUZZ", genericRow(6L, new BigDecimal("10.1"), ImmutableList.of("f", "g"), Collections.emptyMap(), generateStruct(null), generateComplexStruct(5)))
      .build();

  public StructuredTypesDataProvider() {
    super("STRUCTURED_TYPES", PHYSICAL_SCHEMA, ROWS);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> structToMap(final Struct struct) {
    return (Map<String, Object>) structToMapHelper(struct);
  }

  private static Object structToMapHelper(final Object value) {
    if (value instanceof Struct) {
      final Struct struct = (Struct) value;

      final Map<String, Object> result = new HashMap<>();
      for (final Field field : struct.schema().fields()) {
        result.put(field.name(), structToMapHelper(struct.get(field)));
      }

      return result;
    } else if (value instanceof List) {
      final List<?> list = (List<?>) value;

      final List<Object> result = new ArrayList<>();
      for (final Object o : list) {
        result.add(structToMapHelper(o));
      }

      return result;
    } else if (value instanceof Map) {
      final Map<?, ?> map = (Map<?, ?>) value;

      final Map<String, Object> result = new HashMap<>();
      for (final Map.Entry<?, ?> entry : map.entrySet()) {
        result.put(entry.getKey().toString(), structToMapHelper(entry.getValue()));
      }

      return result;
    } else {
      return value;
    }
  }

  private static Struct generateStruct(final Integer value) {
    final Struct struct = new Struct(STRUCT_FIELD_SCHEMA);
    struct.put("F1", value);
    return struct;
  }

  private static Struct generateComplexStruct(final int i) {
    final Struct complexStruct = new Struct(COMPLEX_FIELD_SCHEMA);

    complexStruct.put("DECIMAL", new BigDecimal(i));

    final Struct struct = new Struct(COMPLEX_FIELD_SCHEMA.field("STRUCT").schema());
    struct.put("F1", "v" + i);
    struct.put("F2", i);
    complexStruct.put("STRUCT", struct);

    complexStruct.put("ARRAY_ARRAY", ImmutableList.of(ImmutableList.of("foo")));

    final Struct arrayStruct = new Struct(COMPLEX_FIELD_SCHEMA.field("ARRAY_STRUCT").schema().valueSchema());
    arrayStruct.put("F1", "v" + i);
    complexStruct.put("ARRAY_STRUCT", ImmutableList.of(arrayStruct));

    complexStruct.put("ARRAY_MAP", ImmutableList.of(ImmutableMap.of("k1", i)));

    complexStruct.put("MAP_ARRAY", ImmutableMap.of("k", ImmutableList.of("v" + i)));

    complexStruct.put("MAP_MAP", ImmutableMap.of("k", ImmutableMap.of("k", i)));

    final Struct mapStruct = new Struct(COMPLEX_FIELD_SCHEMA.field("MAP_STRUCT").schema().valueSchema());
    mapStruct.put("F1", "v" + i);
    complexStruct.put("MAP_STRUCT", ImmutableMap.of("k", mapStruct));

    return complexStruct;
  }
}