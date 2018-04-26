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

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import io.confluent.ksql.GenericRow;

public class KeywordDataProvider extends TestDataProvider {

  private static final String namePrefix =
      "KEYWORD";

  private static final String ksqlSchemaString =
      "(`GROUP` bigint, `FROM` varchar, `WHERE` varchar)";

  private static final String key = "GROUP";

  private static final Schema schema = SchemaBuilder.struct()
      .field("GROUP", SchemaBuilder.INT64_SCHEMA)
      .field("FROM", SchemaBuilder.STRING_SCHEMA)
      .field("WHERE", SchemaBuilder.STRING_SCHEMA)
      .build();

  private static final Map<String, GenericRow> data = buildData();

  public KeywordDataProvider() {
    super(namePrefix, ksqlSchemaString, key, schema, data);
  }

  private static Map<String, GenericRow> buildData() {
    return LongStream.range(0, 5)
        .boxed()
        .collect(Collectors.toMap(
            Object::toString,
            idx -> new GenericRow(Arrays.asList(idx, "from_" + idx, "where_" + idx))
        ));
  }
}
