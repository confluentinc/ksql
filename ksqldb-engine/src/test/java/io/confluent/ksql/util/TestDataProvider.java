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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class TestDataProvider<K> {

  private final String topicName;
  private final String key;
  private final PhysicalSchema schema;
  private final Map<K, GenericRow> data;
  private final String kstreamName;

  public TestDataProvider(
      final String namePrefix,
      final String key,
      final PhysicalSchema schema,
      final Map<K, GenericRow> data
  ) {
    this.topicName = Objects.requireNonNull(namePrefix, "namePrefix") + "_TOPIC";
    this.kstreamName = namePrefix + "_KSTREAM";
    this.key = Objects.requireNonNull(key, "key");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.data = Objects.requireNonNull(data, "data");
  }

  public String topicName() {
    return topicName;
  }

  public String ksqlSchemaString(final boolean asTable) {
    return schema.logicalSchema().columns().stream()
        .filter(col -> col.namespace() != Namespace.META)
        .map(col -> col.name() + " " + col.type() + namespace(col.namespace(), asTable))
        .collect(Collectors.joining(", "));
  }

  public String key() {
    return key;
  }

  public PhysicalSchema schema() {
    return schema;
  }

  public Map<K, GenericRow> data() {
    return data;
  }

  public String kstreamName() {
    return kstreamName;
  }

  private static String namespace(final Namespace namespace, final boolean asTable) {
    if (namespace != Namespace.KEY) {
      return "";
    }

    return asTable
        ? " PRIMARY KEY"
        : " KEY";
  }
}
