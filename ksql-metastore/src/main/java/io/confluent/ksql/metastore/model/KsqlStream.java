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

package io.confluent.ksql.metastore.model;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.WindowedSerdes;

@Immutable
public class KsqlStream<K> extends StructuredDataSource<K> {

  public KsqlStream(
      final String sqlExpression,
      final String datasourceName,
      final LogicalSchema schema,
      final Set<SerdeOption> serdeOptions,
      final KeyField keyField,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final KsqlTopic ksqlTopic,
      final SerdeFactory<K> keySerde
  ) {
    super(
        sqlExpression,
        datasourceName,
        schema,
        serdeOptions,
        keyField,
        timestampExtractionPolicy,
        DataSourceType.KSTREAM,
        ksqlTopic,
        keySerde
    );
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " name:" + getName();
  }

  public boolean hasWindowedKey() {
    final Serde<K> keySerde = getKeySerdeFactory().create();
    return keySerde instanceof WindowedSerdes.SessionWindowedSerde
        || keySerde instanceof WindowedSerdes.TimeWindowedSerde;
  }
}
