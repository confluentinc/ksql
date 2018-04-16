/*
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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;

public abstract class StructuredDataSource implements DataSource {

  protected final String dataSourceName;
  protected final DataSourceType dataSourceType;
  protected final Schema schema;
  protected final Field keyField;
  protected final TimestampExtractionPolicy timestampExtractionPolicy;

  protected final KsqlTopic ksqlTopic;
  protected final String sqlExpression;

  public StructuredDataSource(
      final String sqlExpression,
      final String dataSourceName,
      final Schema schema,
      final Field keyField,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final DataSourceType dataSourceType,
      final KsqlTopic ksqlTopic
  ) {
    this.sqlExpression = sqlExpression;
    this.dataSourceName = dataSourceName;
    this.schema = schema;
    this.keyField = keyField;
    this.timestampExtractionPolicy = timestampExtractionPolicy;
    this.dataSourceType = dataSourceType;
    this.ksqlTopic = ksqlTopic;
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
    return this.schema;
  }

  public Field getKeyField() {
    return this.keyField;
  }

  public KsqlTopic getKsqlTopic() {
    return ksqlTopic;
  }

  public TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return timestampExtractionPolicy;
  }

  public abstract StructuredDataSource copy();

  public abstract StructuredDataSource cloneWithTimeKeyColumns();

  public abstract StructuredDataSource cloneWithTimeExtractionPolicy(
      final TimestampExtractionPolicy policy);

  public String getTopicName() {
    return ksqlTopic.getTopicName();
  }

  public abstract QueryId getPersistentQueryId();

  public String getSqlExpression() {
    return sqlExpression;
  }
}
