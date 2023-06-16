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

import static io.confluent.ksql.execution.plan.JoinType.RIGHT;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

public final class StreamStreamJoinBuilder {
  private static final String LEFT_SERDE_CTX = "Left";
  private static final String RIGHT_SERDE_CTX = "Right";

  private StreamStreamJoinBuilder() {
  }

  // deprecation can be fixed after GRACE clause is mandatory
  // (cf. `WithinExpression`)
  @SuppressWarnings("deprecation")
  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> left,
      final KStreamHolder<K> right,
      final StreamStreamJoin<K> join,
      final RuntimeBuildContext buildContext,
      final StreamJoinedFactory streamJoinedFactory) {
    final QueryContext queryContext = join.getProperties().getQueryContext();
    final QueryContext.Stacker stacker = QueryContext.Stacker.of(queryContext);

    final LogicalSchema leftSchema;
    final LogicalSchema rightSchema;
    final Formats rightFormats;
    final Formats leftFormats;
    if (join.getJoinType().equals(RIGHT)) {
      leftFormats = join.getRightInternalFormats();
      rightFormats = join.getLeftInternalFormats();

      leftSchema = right.getSchema();
      rightSchema = left.getSchema();
    } else {
      leftFormats = join.getLeftInternalFormats();
      rightFormats = join.getRightInternalFormats();

      leftSchema = left.getSchema();
      rightSchema = right.getSchema();
    }

    final PhysicalSchema leftPhysicalSchema = PhysicalSchema.from(
        leftSchema,
        leftFormats.getKeyFeatures(),
        leftFormats.getValueFeatures()
    );

    final Serde<GenericRow> leftSerde = buildContext.buildValueSerde(
        leftFormats.getValueFormat(),
        leftPhysicalSchema,
        stacker.push(LEFT_SERDE_CTX).getQueryContext()
    );
    final PhysicalSchema rightPhysicalSchema = PhysicalSchema.from(
        rightSchema,
        rightFormats.getKeyFeatures(),
        rightFormats.getValueFeatures()
    );

    final Serde<GenericRow> rightSerde = buildContext.buildValueSerde(
        rightFormats.getValueFormat(),
        rightPhysicalSchema,
        stacker.push(RIGHT_SERDE_CTX).getQueryContext()
    );
    final Serde<K> keySerde = left.getExecutionKeyFactory().buildKeySerde(
        leftFormats.getKeyFormat(),
        leftPhysicalSchema,
        queryContext
    );
    final StreamJoined<K, GenericRow, GenericRow> joined = streamJoinedFactory.create(
        keySerde,
        leftSerde,
        rightSerde,
        StreamsUtil.buildOpName(queryContext),
        StreamsUtil.buildOpName(queryContext)
    );

    final JoinParams joinParams = JoinParamsFactory
        .create(join.getKeyColName(), leftSchema, rightSchema);

    JoinWindows joinWindows;

    // Grace, as optional, helps to identify if a user specified the GRACE PERIOD syntax in the
    // join window. If specified, then we'll call the new KStreams API ofTimeDifferenceAndGrace()
    // which enables the "spurious" results bugfix with left/outer joins (see KAFKA-10847).
    if (join.getGraceMillis().isPresent()) {
      joinWindows = JoinWindows.ofTimeDifferenceAndGrace(
            join.getBeforeMillis(),
            join.getGraceMillis().get());
    } else {
      joinWindows = JoinWindows.of(join.getBeforeMillis());
    }

    joinWindows = joinWindows.after(join.getAfterMillis());

    final KStream<K, GenericRow> result;
    switch (join.getJoinType()) {
      case LEFT:
        result = left.getStream().leftJoin(
            right.getStream(), joinParams.getJoiner(), joinWindows, joined);
        break;
      case RIGHT:
        result = right.getStream().leftJoin(
            left.getStream(), joinParams.getJoiner(), joinWindows, joined);
        break;
      case OUTER:
        result = left.getStream().outerJoin(
            right.getStream(), joinParams.getJoiner(), joinWindows, joined);
        break;
      case INNER:
        result = left.getStream().join(
            right.getStream(), joinParams.getJoiner(), joinWindows, joined);
        break;
      default:
        throw new IllegalStateException("invalid join type");
    }
    return left.withStream(result, joinParams.getSchema());
  }
}
