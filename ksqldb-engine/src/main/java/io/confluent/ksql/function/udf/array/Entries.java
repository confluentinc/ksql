/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.function.udf.array;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * This UDF constructs an array of structs from the entries in a map. Each struct has a field with
 * name "K" containing the key (this is always a String) and a field with name "V" holding the
 * value;
 */
@UdfDescription(
    name = "ENTRIES",
    category = FunctionCategory.MAP,
    description =
        "Construct an array from the entries in a map."
            + "The array can be optionally sorted on the keys.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Entries {

  private static final Schema INT_STRUCT_SCHEMA = buildStructSchema(Schema.OPTIONAL_INT32_SCHEMA);
  private static final Schema BIGINT_STRUCT_SCHEMA = buildStructSchema(
      Schema.OPTIONAL_INT64_SCHEMA);
  private static final Schema DOUBLE_STRUCT_SCHEMA = buildStructSchema(
      Schema.OPTIONAL_FLOAT64_SCHEMA);
  private static final Schema BOOLEAN_STRUCT_SCHEMA = buildStructSchema(
      Schema.OPTIONAL_BOOLEAN_SCHEMA);
  private static final Schema STRING_STRUCT_SCHEMA = buildStructSchema(
      Schema.OPTIONAL_STRING_SCHEMA);
  private static final String KEY_FIELD_NAME = "K";
  private static final String VALUE_FIELD_NAME = "V";

  private static Schema buildStructSchema(final Schema valueSchema) {
    return SchemaBuilder.struct().field(KEY_FIELD_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .field(VALUE_FIELD_NAME, valueSchema).optional().build();
  }

  @Udf(schema = "ARRAY<STRUCT<K STRING, V INT>>")
  public List<Struct> entriesInt(
      @UdfParameter(description = "The map to create entries from") final Map<String, Integer> map,
      @UdfParameter(description = "If true then the resulting entries are sorted by key")
      final boolean sorted
  ) {
    return entries(map, INT_STRUCT_SCHEMA, sorted);
  }

  @Udf(schema = "ARRAY<STRUCT<K STRING, V BIGINT>>")
  public List<Struct> entriesBigInt(
      @UdfParameter(description = "The map to create entries from") final Map<String, Long> map,
      @UdfParameter(description = "If true then the resulting entries are sorted by key")
      final boolean sorted
  ) {
    return entries(map, BIGINT_STRUCT_SCHEMA, sorted);
  }

  @Udf(schema = "ARRAY<STRUCT<K STRING, V DOUBLE>>")
  public List<Struct> entriesDouble(
      @UdfParameter(description = "The map to create entries from") final Map<String, Double> map,
      @UdfParameter(description = "If true then the resulting entries are sorted by key")
      final boolean sorted
  ) {
    return entries(map, DOUBLE_STRUCT_SCHEMA, sorted);
  }

  @Udf(schema = "ARRAY<STRUCT<K STRING, V BOOLEAN>>")
  public List<Struct> entriesBoolean(
      @UdfParameter(description = "The map to create entries from") final Map<String, Boolean> map,
      @UdfParameter(description = "If true then the resulting entries are sorted by key")
      final boolean sorted
  ) {
    return entries(map, BOOLEAN_STRUCT_SCHEMA, sorted);
  }

  @Udf(schema = "ARRAY<STRUCT<K STRING, V STRING>>")
  public List<Struct> entriesString(
      @UdfParameter(description = "The map to create entries from") final Map<String, String> map,
      @UdfParameter(description = "If true then the resulting entries are sorted by key")
      final boolean sorted
  ) {
    return entries(map, STRING_STRUCT_SCHEMA, sorted);
  }

  private <T> List<Struct> entries(
      final Map<String, T> map, final Schema structSchema, final boolean sorted
  ) {
    if (map == null) {
      return null;
    }
    final List<Struct> structs = new ArrayList<>(map.size());
    Collection<Entry<String, T>> entries = map.entrySet();
    if (sorted) {
      final List<Entry<String, T>> list = new ArrayList<>(entries);
      list.sort(Entry.comparingByKey());
      entries = list;
    }
    for (final Map.Entry<String, T> entry : entries) {
      final Struct struct = new Struct(structSchema);
      struct.put(KEY_FIELD_NAME, entry.getKey()).put(VALUE_FIELD_NAME, entry.getValue());
      structs.add(struct);
    }
    return structs;
  }

}