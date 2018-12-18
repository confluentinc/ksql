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

package io.confluent.ksql.metastore;

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.WindowedSerdes;

public class KsqlStream<K> extends StructuredDataSource {

  private final Serde<K> keySerde;

  public KsqlStream(
      final String sqlExpression,
      final String datasourceName,
      final Schema schema,
      final Field keyField,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final KsqlTopic ksqlTopic,
      final Serde<K> keySerde
  ) {
    super(
        sqlExpression,
        datasourceName,
        schema,
        keyField,
        timestampExtractionPolicy,
        DataSourceType.KSTREAM,
        ksqlTopic
    );
    this.keySerde = Objects.requireNonNull(keySerde, "keySerde");
  }

  public boolean hasWindowedKey() {
    return keySerde instanceof WindowedSerdes.SessionWindowedSerde
        || keySerde instanceof WindowedSerdes.TimeWindowedSerde;
  }

  public Serde<K> getKeySerde() {
    return keySerde;
  }

  @Override
  public StructuredDataSource copy() {
    return new KsqlStream<>(
        sqlExpression,
        dataSourceName,
        schema,
        keyField,
        timestampExtractionPolicy,
        ksqlTopic,
        keySerde
    );
  }

  @Override
  public StructuredDataSource cloneWithTimeKeyColumns() {
    final Schema newSchema = SchemaUtil.addImplicitRowTimeRowKeyToSchema(schema);
    return new KsqlStream<>(
        sqlExpression,
        dataSourceName,
        newSchema,
        keyField,
        timestampExtractionPolicy,
        ksqlTopic,
        keySerde
    );
  }

  @Override
  public StructuredDataSource cloneWithTimeExtractionPolicy(
      final TimestampExtractionPolicy policy) {
    return new KsqlStream<>(
        sqlExpression,
        dataSourceName,
        schema,
        keyField,
        policy,
        ksqlTopic,
        keySerde
    );
  }

  @Override
  public QueryId getPersistentQueryId() {
    return new QueryId("CSAS_" + dataSourceName);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " name:" + getName();
  }

}
