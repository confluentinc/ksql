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

public abstract class StructuredDataSource implements DataSource {

  protected final String dataSourceName;
  protected final DataSourceType dataSourceType;
  protected final Schema schema;
  protected final Field keyField;
  protected final Field timestampField;

  protected final KsqlTopic ksqlTopic;
  protected final boolean sinkTopic;
  protected final String sqlExpression;

  public StructuredDataSource(
      final String sqlExpression,
      final String dataSourceName,
      final Schema schema,
      final Field keyField,
      final Field timestampField,
      final DataSourceType dataSourceType,
      final KsqlTopic ksqlTopic,
      final boolean sinkTopic
  ) {
    this.sqlExpression = sqlExpression;
    this.dataSourceName = dataSourceName;
    this.schema = schema;
    this.keyField = keyField;
    this.timestampField = timestampField;
    this.dataSourceType = dataSourceType;
    this.ksqlTopic = ksqlTopic;
    this.sinkTopic = sinkTopic;
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

  public boolean isSinkTopic() {
    return sinkTopic;
  }

  public Field getTimestampField() {
    return timestampField;
  }

  public abstract StructuredDataSource cloneWithTimeKeyColumns();

  public abstract StructuredDataSource cloneWithTimeField(String timestampfieldName);

  public String getTopicName() {
    return ksqlTopic.getTopicName();
  }

  public abstract QueryId getPersistentQueryId();

  public String getSqlExpression() {
    return sqlExpression;
  }
}
