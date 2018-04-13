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
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;

public class KsqlStdOut extends StructuredDataSource {

  public static final String KSQL_STDOUT_NAME = "KSQL_STDOUT_NAME";

  public KsqlStdOut(
      final String datasourceName,
      final Schema schema,
      final Field keyField,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final DataSourceType dataSourceType
  ) {
    super(
        "not-applicable-for-stdout",
        datasourceName,
        schema,
        keyField,
        timestampExtractionPolicy,
        dataSourceType,
        null
    );
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }

  @Override
  public Field getKeyField() {
    return null;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return null;
  }

  @Override
  public StructuredDataSource copy() {
    return this;
  }

  @Override
  public StructuredDataSource cloneWithTimeKeyColumns() {
    return this;
  }

  @Override
  public StructuredDataSource cloneWithTimeExtractionPolicy(TimestampExtractionPolicy policy) {
    return this;
  }

  @Override
  public QueryId getPersistentQueryId() {
    throw new UnsupportedOperationException("KsqlStdOut doesn't support persistent queries");
  }
}
