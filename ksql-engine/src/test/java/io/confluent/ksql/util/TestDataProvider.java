/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

public abstract class TestDataProvider {
  final String topicName;
  final String ksqlSchemaString;
  final String key;
  final Schema schema;
  final Map<String, GenericRow> data;
  final String kstreamName;

  public TestDataProvider(String namePrefix, String ksqlSchemaString, String key, Schema schema, Map<String, GenericRow> data) {
    this.topicName = namePrefix + "_TOPIC";
    this.kstreamName =  namePrefix + "_KSTREAM";
    this.ksqlSchemaString = ksqlSchemaString;
    this.key = key;
    this.schema = schema;
    this.data = data;
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
