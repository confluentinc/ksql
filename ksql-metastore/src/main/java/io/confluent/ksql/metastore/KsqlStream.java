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

package io.confluent.ksql.metastore;

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

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

  public boolean isWindowed() {
    return !(keySerde instanceof Serdes.StringSerde);
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
