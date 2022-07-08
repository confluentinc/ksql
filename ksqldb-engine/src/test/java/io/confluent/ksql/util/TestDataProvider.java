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

import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;

public class TestDataProvider {

  private final String topicName;
  private final PhysicalSchema schema;
  private final Multimap<GenericKey, GenericRow> data;
  private final String sourceName;

  public TestDataProvider(
      final String namePrefix,
      final PhysicalSchema schema,
      final Multimap<GenericKey, GenericRow> data
  ) {
    this.topicName = Objects.requireNonNull(namePrefix, "namePrefix") + "_TOPIC";
    this.sourceName = namePrefix + "_KSTREAM";
    this.schema = Objects.requireNonNull(schema, "schema");
    this.data = Multimaps.unmodifiableMultimap(
        LinkedListMultimap.create(Objects.requireNonNull(data, "data"))
    );
  }

  public String topicName() {
    return topicName;
  }

  public String ksqlSchemaString(final boolean asTable) {
    return schema.logicalSchema().columns().stream()
        .map(col ->
            col.name() + " " + col.type() + namespace(col.namespace(), asTable, col.headerKey()))
        .collect(Collectors.joining(", "));
  }

  public String key() {
    return Iterables.getOnlyElement(schema.logicalSchema().key()).name().text();
  }

  public PhysicalSchema schema() {
    return schema;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "data is unmodifiableMultimap()")
  public Multimap<GenericKey, GenericRow> data() {
    return data;
  }

  public Map<GenericKey, GenericRow> finalData() {
    return data.entries().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            Entry::getValue,
            (first, second) -> second
        ));
  }

  public String sourceName() {
    return sourceName;
  }

  private static String namespace(
      final Namespace namespace, final boolean asTable, final Optional<String> headerKey) {
    if (namespace == Namespace.VALUE) {
      return "";
    } else if (namespace == Namespace.HEADERS) {
      return headerKey.isPresent()
          ? " HEADER('" + headerKey.get() + "')"
          : " HEADERS";
    }

    return asTable
        ? " PRIMARY KEY"
        : " KEY";
  }

  static <K> KeyValue<K, GenericRow> kv(final K k, GenericRow v) {
    return new KeyValue<>(k, v);
  }
}
