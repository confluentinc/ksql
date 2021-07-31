/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.client;

import static io.confluent.ksql.rest.client.KsqlClientUtil.deserialize;

import com.google.common.base.Strings;
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.Pair;
import io.vertx.core.buffer.Buffer;
import java.util.List;
import java.util.stream.Collectors;

public final class KsqlTargetUtil {

  private KsqlTargetUtil() {

  }

  public static StreamedRow toRowFromDelimited(final Buffer buff) {
    try {
      final QueryResponseMetadata metadata = deserialize(buff, QueryResponseMetadata.class);
      return StreamedRow.header(new QueryId(Strings.nullToEmpty(metadata.queryId)),
          createSchema(metadata));
    } catch (KsqlRestClientException e) {
      // Not a {@link QueryResponseMetadata}
    }
    try {
      final KsqlErrorMessage error = deserialize(buff, KsqlErrorMessage.class);
      return StreamedRow.error(new RuntimeException(error.getMessage()), error.getErrorCode());
    } catch (KsqlRestClientException e) {
      // Not a {@link KsqlErrorMessage}
    }
    try {
      final List<?> row = deserialize(buff, List.class);
      return StreamedRow.pushRow(GenericRow.fromList(row));
    } catch (KsqlRestClientException e) {
      // Not a {@link List}
    }
    throw new IllegalStateException("Couldn't parse message: " + buff.toString());
  }

  private static LogicalSchema createSchema(final QueryResponseMetadata metadata) {
    final SqlTypeParser parser = SqlTypeParser.create(TypeRegistry.EMPTY);
    return LogicalSchema.builder().valueColumns(
        Streams.zip(metadata.columnNames.stream(), metadata.columnTypes.stream(), Pair::of)
            .map(pair -> {
              final SqlType sqlType = parser.parse(pair.getRight()).getSqlType();
              final ColumnName name = ColumnName.of(pair.getLeft());
              return new SimpleColumn() {
                @Override
                public ColumnName name() {
                  return name;
                }

                @Override
                public SqlType type() {
                  return sqlType;
                }
              };
            }).collect(Collectors.toList()))
        .build();
  }
}
