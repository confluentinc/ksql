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

import static io.confluent.ksql.GenericKey.genericKey;
import static io.confluent.ksql.GenericRow.genericRow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class StructuredTypesDataProvider extends TestDataProvider {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K"), SqlTypes.struct()
          .field("F1", SqlTypes.array(SqlTypes.STRING))
          .build())
      .valueColumn(ColumnName.of("STR"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("LONG"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("DEC"), SqlTypes.decimal(4, 2))
      .valueColumn(ColumnName.of("BYTES_"), SqlTypes.BYTES)
      .valueColumn(ColumnName.of("ARRAY"), SqlTypes.array(SqlTypes.STRING))
      .valueColumn(ColumnName.of("MAP"), SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING))
      .valueColumn(ColumnName.of("STRUCT"), SqlTypes.struct().field("F1", SqlTypes.INTEGER).build())
      .valueColumn(ColumnName.of("COMPLEX"), SqlTypes.struct()
          .field("DECIMAL", SqlTypes.decimal(2, 1))
          .field("STRUCT", SqlTypes.struct()
              .field("F1", SqlTypes.STRING)
              .field("F2", SqlTypes.INTEGER)
              .build())
          .field("ARRAY_ARRAY", SqlTypes.array(SqlTypes.array(SqlTypes.STRING)))
          .field("ARRAY_STRUCT", SqlTypes.array(SqlTypes.struct().field("F1", SqlTypes.STRING).build()))
          .field("ARRAY_MAP", SqlTypes.array(SqlTypes.map(SqlTypes.STRING, SqlTypes.INTEGER)))
          .field("MAP_ARRAY", SqlTypes.map(SqlTypes.STRING, SqlTypes.array(SqlTypes.STRING)))
          .field("MAP_MAP", SqlTypes.map(SqlTypes.STRING,
              SqlTypes.map(SqlTypes.STRING, SqlTypes.INTEGER)
          ))
          .field("MAP_STRUCT", SqlTypes.map(SqlTypes.STRING,
              SqlTypes.struct().field("F1", SqlTypes.STRING).build()
          ))
          .build()
      )
      .valueColumn(ColumnName.of("TIMESTAMP"), SqlTypes.TIMESTAMP)
      .valueColumn(ColumnName.of("DATE"), SqlTypes.DATE)
      .valueColumn(ColumnName.of("TIME"), SqlTypes.TIME)
      .headerColumn(ColumnName.of("HEAD"), Optional.of("h0"))
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(LOGICAL_SCHEMA, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES), SerdeFeatures.of());

  private static final Schema KEY_FIELD_SCHEMA = ConnectSchemas.columnsToConnectSchema(LOGICAL_SCHEMA.key())
      .field("K").schema();

  private static final ConnectSchema VALUE_CONNECT_SCHEMA = ConnectSchemas.columnsToConnectSchema(LOGICAL_SCHEMA.value());
  private static final Schema STRUCT_FIELD_SCHEMA = VALUE_CONNECT_SCHEMA.field("STRUCT").schema();
  private static final Schema COMPLEX_FIELD_SCHEMA = VALUE_CONNECT_SCHEMA.field("COMPLEX").schema();

  private static final Multimap<GenericKey, GenericRow> ROWS = ImmutableListMultimap
      .<GenericKey, GenericRow>builder()
      .put(genericKey(generateStructKey("a")), genericRow("FOO", 1L, new BigDecimal("1.11"), new byte[]{1},
          Collections.singletonList("a"), Collections.singletonMap("k1", "v1"), generateSimpleStructValue(2), generateComplexStructValue(0),
          new Timestamp(1), new Date(86400000), new Time(0)))
      .put(genericKey(generateStructKey("b")), genericRow("BAR", 2L, new BigDecimal("2.22"), new byte[]{2},
          Collections.emptyList(), Collections.emptyMap(), generateSimpleStructValue(3), generateComplexStructValue(1),
          new Timestamp(2), new Date(86400000 * 2), new Time(1)))
      .put(genericKey(generateStructKey("c")), genericRow("BAZ", 3L, new BigDecimal("30.33"), new byte[]{3},
          Collections.singletonList("b"), Collections.emptyMap(), generateSimpleStructValue(null), generateComplexStructValue(2),
          new Timestamp(3), new Date(86400000 * 3), new Time(2)))
      .put(genericKey(generateStructKey("d")), genericRow("BUZZ", 4L, new BigDecimal("40.44"), new byte[]{4},
          ImmutableList.of("c", "d"), Collections.emptyMap(), generateSimpleStructValue(88), generateComplexStructValue(3),
          new Timestamp(4), new Date(86400000 * 4), new Time(3)))
      // Additional entries for repeated keys
      .put(genericKey(generateStructKey("c")), genericRow("BAZ", 5L, new BigDecimal("12.0"), new byte[]{15},
          ImmutableList.of("e"), ImmutableMap.of("k1", "v1", "k2", "v2"), generateSimpleStructValue(0), generateComplexStructValue(4),
          new Timestamp(11), new Date(86400000 * 11), new Time(11)))
      .put(genericKey(generateStructKey("d")), genericRow("BUZZ", 6L, new BigDecimal("10.1"), new byte[]{6},
          ImmutableList.of("f", "g"), Collections.emptyMap(), generateSimpleStructValue(null), generateComplexStructValue(5),
          new Timestamp(12), new Date(86400000 * 12), new Time(12)))
      .build();

  private static final Multimap<GenericKey, GenericRow> ROWS2 = ImmutableListMultimap
      .<GenericKey, GenericRow>builder()
      .put(genericKey(generateStructKey("e")), genericRow("FOO", 7L, new BigDecimal("1.11"), new byte[]{1},
          Collections.singletonList("a"), Collections.singletonMap("k1", "v1"), generateSimpleStructValue(2), generateComplexStructValue(0),
          new Timestamp(1), new Date(86400000), new Time(0)))
      .put(genericKey(generateStructKey("f")), genericRow("BAR", 8L, new BigDecimal("2.22"), new byte[]{2},
          Collections.emptyList(), Collections.emptyMap(), generateSimpleStructValue(3), generateComplexStructValue(1),
          new Timestamp(2), new Date(86400000 * 2), new Time(1)))
      .put(genericKey(generateStructKey("g")), genericRow("BAZ", 9L, new BigDecimal("30.33"), new byte[]{3},
          Collections.singletonList("b"), Collections.emptyMap(), generateSimpleStructValue(null), generateComplexStructValue(2),
          new Timestamp(3), new Date(86400000 * 3), new Time(2)))
      .put(genericKey(generateStructKey("h")), genericRow("BUZZ", 10L, new BigDecimal("40.44"), new byte[]{4},
          ImmutableList.of("c", "d"), Collections.emptyMap(), generateSimpleStructValue(88), generateComplexStructValue(3),
          new Timestamp(4), new Date(86400000 * 4), new Time(3)))
      // Additional entries for repeated keys
      .put(genericKey(generateStructKey("i")), genericRow("BAZ", 11L, new BigDecimal("12.0"), new byte[]{15},
          ImmutableList.of("e"), ImmutableMap.of("k1", "v1", "k2", "v2"), generateSimpleStructValue(0), generateComplexStructValue(4),
          new Timestamp(11), new Date(86400000 * 11), new Time(11)))
      .put(genericKey(generateStructKey("j")), genericRow("BUZZ", 12L, new BigDecimal("10.1"), new byte[]{6},
          ImmutableList.of("f", "g"), Collections.emptyMap(), generateSimpleStructValue(null), generateComplexStructValue(5),
          new Timestamp(12), new Date(86400000 * 12), new Time(12)))
      .build();

  public StructuredTypesDataProvider() {
    this("STRUCTURED_TYPES");
  }
  public StructuredTypesDataProvider(final Batch batch) {
    super("STRUCTURED_TYPES", PHYSICAL_SCHEMA, batch.rows);
  }

  public enum Batch {
    BATCH1(ROWS),
    BATCH2(ROWS2);

    private final Multimap<GenericKey, GenericRow> rows;

    Batch(final Multimap<GenericKey, GenericRow> rows) {
      this.rows = rows;
    }
  }

  public StructuredTypesDataProvider(final String namePrefix) {
    super(namePrefix, PHYSICAL_SCHEMA, ROWS);
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

  private static Struct generateStructKey(final String value) {
    final Struct struct = new Struct(KEY_FIELD_SCHEMA);
    struct.put("F1", ImmutableList.of(value));
    return struct;
  }

  private static Struct generateSimpleStructValue(final Integer value) {
    final Struct struct = new Struct(STRUCT_FIELD_SCHEMA);
    struct.put("F1", value);
    return struct;
  }

  private static Struct generateComplexStructValue(final int i) {
    final Struct complexStruct = new Struct(COMPLEX_FIELD_SCHEMA);

    complexStruct.put("DECIMAL", new BigDecimal(i).setScale(1, RoundingMode.UNNECESSARY));

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