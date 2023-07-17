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
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Optional;

@Immutable
public class KsqlStream<K> extends StructuredDataSource<K> {

  public KsqlStream(
      final String sqlExpression,
      final SourceName datasourceName,
      final LogicalSchema schema,
      final Optional<TimestampColumn> timestampExtractionPolicy,
      final boolean isKsqlSink,
      final KsqlTopic ksqlTopic,
      final boolean isSourceStream
  ) {
    super(
        sqlExpression,
        datasourceName,
        schema,
        timestampExtractionPolicy,
        DataSourceType.KSTREAM,
        isKsqlSink,
        ksqlTopic,
        isSourceStream
    );
  }

  @Override
  public DataSource with(final String sql, final LogicalSchema schema) {
    return new KsqlStream<>(
        getSqlExpression() + '\n' + sql,
        getName(),
        schema,
        getTimestampColumn(),
        isCasTarget(),
        getKsqlTopic(),
        isSource()
    );
  }
}
