/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KsqlBareOutputNode extends OutputNode {

  @JsonCreator
  public KsqlBareOutputNode(@JsonProperty("id") final PlanNodeId id,
                            @JsonProperty("source") final PlanNode source,
                            @JsonProperty("schema") final Schema schema,
                            @JsonProperty("limit") final Optional<Integer> limit,
                            @JsonProperty("timestampExtraction")
                              final TimestampExtractionPolicy extractionPolicy) {
    super(id, source, schema, limit, extractionPolicy);
  }

  public String getKafkaTopicName() {
    return null;
  }

  @Override
  public QueryId getQueryId(final QueryIdGenerator queryIdGenerator) {
    return new QueryId(String.valueOf(Math.abs(ThreadLocalRandom.current().nextLong())));
  }

  @Override
  public Field getKeyField() {
    return null;
  }

  @Override
  public SchemaKStream<?> buildStream(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry,
      final QueryId queryId) {
    final SchemaKStream schemaKStream = getSource().buildStream(
        builder,
        ksqlConfig,
        serviceContext,
        functionRegistry,
        queryId);

    schemaKStream.setOutputNode(this);
    return schemaKStream.toQueue(buildNodeContext(queryId));
  }
}
