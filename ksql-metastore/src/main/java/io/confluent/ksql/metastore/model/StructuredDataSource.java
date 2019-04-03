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

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.schema.ksql.KsqlSchema;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

@Immutable
public abstract class StructuredDataSource<K> implements DataSource {

  private final String dataSourceName;
  private final DataSourceType dataSourceType;
  private final KsqlSchema schema;
  private final Optional<Field> keyField;
  private final TimestampExtractionPolicy timestampExtractionPolicy;
  private final SerdeFactory<K> keySerde;
  private final KsqlTopic ksqlTopic;
  private final String sqlExpression;

  public StructuredDataSource(
      final String sqlExpression,
      final String dataSourceName,
      final Schema schema,
      final Optional<Field> keyField,
      final TimestampExtractionPolicy tsExtractionPolicy,
      final DataSourceType dataSourceType,
      final KsqlTopic ksqlTopic,
      final SerdeFactory<K> keySerde
  ) {
    this.sqlExpression = requireNonNull(sqlExpression, "sqlExpression");
    this.dataSourceName = requireNonNull(dataSourceName, "dataSourceName");
    this.schema = KsqlSchema.of(schema);
    this.keyField = requireNonNull(keyField, "keyField");
    this.timestampExtractionPolicy = requireNonNull(tsExtractionPolicy, "tsExtractionPolicy");
    this.dataSourceType = requireNonNull(dataSourceType, "dataSourceType");
    this.ksqlTopic = requireNonNull(ksqlTopic, "ksqlTopic");
    this.keySerde = requireNonNull(keySerde, "keySerde");
  }

  @Override
  public String getName() {
    return this.dataSourceName;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return this.dataSourceType;
  }

  public Schema getSchema() {
    return schema.getSchema();
  }

  public Optional<Field> getKeyField() {
    return keyField;
  }

  public KsqlTopic getKsqlTopic() {
    return ksqlTopic;
  }

  public SerdeFactory<K> getKeySerdeFactory() {
    return keySerde;
  }

  public KsqlTopicSerDe getKsqlTopicSerde() {
    return ksqlTopic.getKsqlTopicSerDe();
  }

  public boolean isSerdeFormat(final DataSource.DataSourceSerDe format) {
    return getKsqlTopicSerde() != null && getKsqlTopicSerde().getSerDe() == format;
  }

  public TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return timestampExtractionPolicy;
  }

  public String getKsqlTopicName() {
    return ksqlTopic.getKsqlTopicName();
  }

  public String getKafkaTopicName() {
    return ksqlTopic.getKafkaTopicName();
  }

  public String getSqlExpression() {
    return sqlExpression;
  }
}
