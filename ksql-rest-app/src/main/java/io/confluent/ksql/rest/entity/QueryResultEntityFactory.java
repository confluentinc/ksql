/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import io.confluent.ksql.materialization.TableRow;
import io.confluent.ksql.materialization.Window;
import io.confluent.ksql.rest.entity.QueryResultEntity.ResultRow;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Struct;

/**
 * Factory class for {@link QueryResultEntity}
 */
public final class QueryResultEntityFactory {

  private QueryResultEntityFactory() {
  }

  public static List<ResultRow> createRows(
      final List<? extends TableRow> result
  ) {
    return result.stream()
        .map(QueryResultEntityFactory::createRow)
        .collect(Collectors.toList());
  }

  private static ResultRow createRow(final TableRow row) {
    final List<Object> values = new ArrayList<>();
    keyFields(row.key()).forEach(values::add);
    values.addAll(row.value().getColumns());

    return ResultRow.of(
        row.window().map(QueryResultEntityFactory::window),
        values
    );
  }

  private static Stream<?> keyFields(final Struct key) {
    return key.schema().fields().stream().map(key::get);
  }

  private static QueryResultEntity.Window window(final Window w) {
    return new QueryResultEntity.Window(
        w.start()
            .toEpochMilli(),
        w.end()
            .map(Instant::toEpochMilli)
            .map(OptionalLong::of)
            .orElse(OptionalLong.empty())
    );
  }
}
