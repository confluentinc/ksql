/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;

public interface KeySerdeFactory<K> {
  KeySerde<K> buildKeySerde(
      KeyFormat keyFormat,
      PhysicalSchema physicalSchema,
      QueryContext queryContext
  );

  static KeySerdeFactory<Struct> unwindowed(final KsqlQueryBuilder queryBuilder) {
    return (fmt, schema, ctx) ->
        queryBuilder.buildKeySerde(fmt.getFormatInfo(), schema, ctx);
  }

  static KeySerdeFactory<Windowed<Struct>> windowed(final KsqlQueryBuilder queryBuilder) {
    return (fmt, schema, ctx) ->
        queryBuilder.buildKeySerde(
            fmt.getFormatInfo(),
            fmt.getWindowInfo().get(),
            schema,
            ctx
        );
  }
}
