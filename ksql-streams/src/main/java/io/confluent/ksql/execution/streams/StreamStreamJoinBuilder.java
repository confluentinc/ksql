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
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeySerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;

public final class StreamStreamJoinBuilder {
  private static final String LEFT_SERDE_CTX = "left";
  private static final String RIGHT_SERDE_CTX = "right";

  private StreamStreamJoinBuilder() {
  }

  public static <K> KStream<K, GenericRow> build(
      final KStream<K, GenericRow> left,
      final KStream<K, GenericRow> right,
      final StreamStreamJoin<K> join,
      final KeySerdeFactory<K> keySerdeFactory,
      final KsqlQueryBuilder queryBuilder,
      final JoinedFactory joinedFactory) {
    final Formats leftFormats = join.getLeftFormats();
    final QueryContext queryContext = join.getProperties().getQueryContext();
    final QueryContext.Stacker stacker = QueryContext.Stacker.of(queryContext);
    final LogicalSchema leftSchema = join.getLeft().getProperties().getSchema();
    final PhysicalSchema leftPhysicalSchema = PhysicalSchema.from(
        leftSchema.withoutAlias(),
        leftFormats.getOptions()
    );
    final Serde<GenericRow> leftSerde = queryBuilder.buildValueSerde(
        leftFormats.getValueFormat().getFormatInfo(),
        leftPhysicalSchema,
        stacker.push(LEFT_SERDE_CTX).getQueryContext()
    );
    final Formats rightFormats = join.getRightFormats();
    final LogicalSchema rightSchema = join.getRight().getProperties().getSchema();
    final PhysicalSchema rightPhysicalSchema = PhysicalSchema.from(
        rightSchema.withoutAlias(),
        rightFormats.getOptions()
    );
    final Serde<GenericRow> rightSerde = queryBuilder.buildValueSerde(
        rightFormats.getValueFormat().getFormatInfo(),
        rightPhysicalSchema,
        stacker.push(RIGHT_SERDE_CTX).getQueryContext()
    );
    final KeySerde<K> keySerde = keySerdeFactory.buildKeySerde(
        leftFormats.getKeyFormat(),
        leftPhysicalSchema,
        queryContext
    );
    final Joined<K, GenericRow, GenericRow> joined = joinedFactory.create(
        keySerde,
        leftSerde,
        rightSerde,
        StreamsUtil.buildOpName(queryContext)
    );
    final KsqlValueJoiner joiner = new KsqlValueJoiner(leftSchema, rightSchema);
    final JoinWindows joinWindows = JoinWindows.of(join.getBefore()).after(join.getAfter());
    switch (join.getJoinType()) {
      case LEFT:
        return left.leftJoin(right, joiner, joinWindows, joined);
      case OUTER:
        return left.outerJoin(right, joiner, joinWindows, joined);
      case INNER:
        return left.join(right, joiner, joinWindows, joined);
      default:
        throw new IllegalStateException("invalid join type");
    }
  }
}
