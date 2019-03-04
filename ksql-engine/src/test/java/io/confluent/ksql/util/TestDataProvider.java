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
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;

public abstract class TestDataProvider {
  private final String topicName;
  private final String ksqlSchemaString;
  private final String key;
  private final Schema schema;
  private final Map<String, GenericRow> data;
  private final String kstreamName;

  TestDataProvider(
      final String namePrefix,
      final String ksqlSchemaString,
      final String key,
      final Schema schema,
      final Map<String, GenericRow> data
  ) {
    this.topicName = Objects.requireNonNull(namePrefix, "namePrefix") + "_TOPIC";
    this.kstreamName =  namePrefix + "_KSTREAM";
    this.ksqlSchemaString = Objects.requireNonNull(ksqlSchemaString, "ksqlSchemaString");
    this.key = Objects.requireNonNull(key, "key");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.data = Objects.requireNonNull(data, "data");
  }

  public String topicName() {
    return topicName;
  }

  public String ksqlSchemaString() {
    return ksqlSchemaString;
  }

  public String key() {
    return key;
  }

  public Schema schema() {
    return schema;
  }

  public Map<String, GenericRow> data() {
    return data;
  }

  public String kstreamName() {
    return kstreamName;
  }
}
