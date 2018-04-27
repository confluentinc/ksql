/*
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
 */

package io.confluent.ksql.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import io.confluent.ksql.GenericRow;

public class MixedCaseDataProvider extends TestDataProvider {

  private static final String namePrefix =
      "MIXED_CASE";

  private static final String ksqlSchemaString =
      "("
      + "\"thing1\" varchar, `thing2` varchar,"
      + "\"Thing1\" varchar, `Thing2` varchar,"
      + "\"THING1\" varchar, `THING2` varchar"
      + ")";

  private static final String key = "col1";

  // Deliberately uses a mix of case:
  private static final Schema schema = SchemaBuilder.struct()
      .field("thing1", SchemaBuilder.STRING_SCHEMA)
      .field("thing2", SchemaBuilder.STRING_SCHEMA)
      .field("Thing1", SchemaBuilder.STRING_SCHEMA)
      .field("Thing2", SchemaBuilder.STRING_SCHEMA)
      .field("THING1", SchemaBuilder.STRING_SCHEMA)
      .field("THING2", SchemaBuilder.STRING_SCHEMA)
      .build();

  private static final Map<String, GenericRow> data = buildData();

  public MixedCaseDataProvider() {
    super(namePrefix, ksqlSchemaString, key, schema, data);
  }

  private static Map<String, GenericRow> buildData() {
    final Function<Long, GenericRow> rowGenerator = idx -> {
      final List<Object> values = schema.fields().stream()
          .map(f -> f.name() + "_" + idx)
          .collect(Collectors.toList());
      return new GenericRow(values);
    };

    return LongStream.range(0, 5)
        .boxed()
        .collect(Collectors.toMap(Object::toString, rowGenerator));
  }
}
