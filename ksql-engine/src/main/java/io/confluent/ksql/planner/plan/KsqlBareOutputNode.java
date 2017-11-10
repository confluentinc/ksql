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

package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Map;
import java.util.Optional;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;

public class KsqlBareOutputNode extends OutputNode {

  @JsonCreator
  public KsqlBareOutputNode(@JsonProperty("id") final PlanNodeId id,
                            @JsonProperty("source") final PlanNode source,
                            @JsonProperty("schema") final Schema schema,
                            @JsonProperty("limit") final Optional<Integer> limit) {
    super(id, source, schema, limit);


  }

  public String getKafkaTopicName() {
    return null;
  }

  @Override
  public Field getKeyField() {
    return null;
  }

  @Override
  public SchemaKStream buildStream(final StreamsBuilder builder,
                                   final KsqlConfig ksqlConfig,
                                   final KafkaTopicClient kafkaTopicClient,
                                   final MetastoreUtil metastoreUtil,
                                   final FunctionRegistry functionRegistry,
                                   final Map<String, Object> props) {
    final SchemaKStream schemaKStream = getSource().buildStream(builder,
        ksqlConfig,
        kafkaTopicClient,
        metastoreUtil,
        functionRegistry,
        props);

    schemaKStream.setOutputNode(this);
    return schemaKStream.toQueue(getLimit());
  }
}
