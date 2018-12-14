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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KsqlTable extends StructuredDataSource {

  private final String stateStoreName;
  private final boolean isWindowed;

  public KsqlTable(
      final String sqlExpression,
      final String datasourceName,
      final Schema schema,
      final Field keyField,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final KsqlTopic ksqlTopic,
      final String stateStoreName,
      final boolean isWindowed
  ) {
    super(
        sqlExpression,
        datasourceName,
        schema,
        keyField,
        timestampExtractionPolicy,
        DataSourceType.KTABLE,
        ksqlTopic
    );
    this.stateStoreName = stateStoreName;
    this.isWindowed = isWindowed;
  }

  public boolean isWindowed() {
    return isWindowed;
  }

  @Override
  public StructuredDataSource copy() {
    return new KsqlTable(
        sqlExpression,
        dataSourceName,
        schema,
        keyField,
        timestampExtractionPolicy,
        ksqlTopic,
        stateStoreName,
        isWindowed
    );
  }

  @Override
  public StructuredDataSource cloneWithTimeKeyColumns() {
    final Schema newSchema = SchemaUtil.addImplicitRowTimeRowKeyToSchema(schema);
    return new KsqlTable(
        sqlExpression,
        dataSourceName,
        newSchema,
        keyField,
        timestampExtractionPolicy,
        ksqlTopic,
        stateStoreName,
        isWindowed
    );
  }

  @Override
  public StructuredDataSource cloneWithTimeExtractionPolicy(
      final TimestampExtractionPolicy policy) {
    return new KsqlTable(
        sqlExpression,
        dataSourceName,
        schema,
        keyField,
        policy,
        ksqlTopic,
        stateStoreName,
        isWindowed
    );
  }

  @Override
  public QueryId getPersistentQueryId() {
    return new QueryId("CTAS_" + dataSourceName);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " name:" + getName();
  }
}
