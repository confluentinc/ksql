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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;

public class KsqlStream extends StructuredDataSource {

  public KsqlStream(
      String sqlExpression,
      final String datasourceName,
      final Schema schema,
      final Field keyField,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final KsqlTopic ksqlTopic
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
  }

  @Override
  public StructuredDataSource copy() {
    return new KsqlStream(
        sqlExpression,
        dataSourceName,
        schema,
        keyField,
        timestampExtractionPolicy,
        ksqlTopic
    );
  }

  @Override
  public StructuredDataSource cloneWithTimeKeyColumns() {
    Schema newSchema = SchemaUtil.addImplicitRowTimeRowKeyToSchema(schema);
    return new KsqlStream(
        sqlExpression,
        dataSourceName,
        newSchema,
        keyField,
        timestampExtractionPolicy,
        ksqlTopic
    );
  }

  @Override
  public StructuredDataSource cloneWithTimeExtractionPolicy(
      final TimestampExtractionPolicy policy) {
    return new KsqlStream(
        sqlExpression,
        dataSourceName,
        schema,
        keyField,
        policy,
        ksqlTopic
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
