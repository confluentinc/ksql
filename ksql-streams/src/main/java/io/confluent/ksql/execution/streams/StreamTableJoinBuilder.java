/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeySerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;

public final class StreamTableJoinBuilder {
  private static final String SERDE_CTX = "left";

  private StreamTableJoinBuilder() {
  }

  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> left,
      final KTableHolder<K> right,
      final StreamTableJoin<K> join,
      final KsqlQueryBuilder queryBuilder,
      final JoinedFactory joinedFactory) {
    final Formats leftFormats = join.getFormats();
    final QueryContext queryContext = join.getProperties().getQueryContext();
    final QueryContext.Stacker stacker = QueryContext.Stacker.of(queryContext);
    final LogicalSchema leftSchema = left.getSchema();
    final PhysicalSchema leftPhysicalSchema = PhysicalSchema.from(
        leftSchema.withoutAlias(),
        leftFormats.getOptions()
    );
    final Serde<GenericRow> leftSerde = queryBuilder.buildValueSerde(
        leftFormats.getValueFormat(),
        leftPhysicalSchema,
        stacker.push(SERDE_CTX).getQueryContext()
    );
    final KeySerde<K> keySerde = left.getKeySerdeFactory().buildKeySerde(
        leftFormats.getKeyFormat(),
        leftPhysicalSchema,
        queryContext
    );
    final Joined<K, GenericRow, GenericRow> joined = joinedFactory.create(
        keySerde,
        leftSerde,
        null,
        StreamsUtil.buildOpName(queryContext)
    );
    final LogicalSchema rightSchema = right.getSchema();
    final JoinParams joinParams = JoinParamsFactory.create(leftSchema, rightSchema);
    final KStream<K, GenericRow> result;
    switch (join.getJoinType()) {
      case LEFT:
        result = left.getStream().leftJoin(right.getTable(), joinParams.getJoiner(), joined);
        break;
      case INNER:
        result = left.getStream().join(right.getTable(), joinParams.getJoiner(), joined);
        break;
      default:
        throw new IllegalStateException("invalid join type");
    }
    return left.withStream(result, joinParams.getSchema());
  }
}