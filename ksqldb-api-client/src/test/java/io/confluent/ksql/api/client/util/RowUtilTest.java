/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.client.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.api.client.ColumnType;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class RowUtilTest {

  @Test
  public void shouldGetColumnTypesFromStrings() {
    // Given
    final List<String> stringTypes = ImmutableList.of(
        "STRING",
        "INTEGER",
        "BIGINT",
        "BOOLEAN",
        "DOUBLE",
        "ARRAY<STRING>",
        "MAP<STRING, STRING>",
        "DECIMAL(4, 2)",
        "STRUCT<`F1` STRING, `F2` INTEGER>",
        "TIMESTAMP",
        "DATE",
        "TIME"
    );

    // When
    final List<ColumnType> columnTypes = RowUtil.columnTypesFromStrings(stringTypes);

    // Then
    assertThat(
        columnTypes.stream()
            .map(t -> t.getType().toString())
            .collect(Collectors.toList()),
        contains(
            "STRING",
            "INTEGER",
            "BIGINT",
            "BOOLEAN",
            "DOUBLE",
            "ARRAY",
            "MAP",
            "DECIMAL",
            "STRUCT",
            "TIMESTAMP",
            "DATE",
            "TIME"
    ));
  }

  @Test
  public void shouldGetColumnNamesFromSchema() {
    // Given
    final String schema = "`K` STRUCT<`F1` ARRAY<STRING>>, "
            + "`STR` STRING, "
            + "`LONG` BIGINT, "
            + "`DEC` DECIMAL(4, 2),"
            + "`BYTES_` BYTES, "
            + "`ARRAY` ARRAY<STRING>, "
            + "`MAP` MAP<STRING, STRING>, "
            + "`STRUCT` STRUCT<`F1` INTEGER>, "
            + "`COMPLEX` STRUCT<`DECIMAL` DECIMAL(2, 1), `STRUCT` STRUCT<`F1` STRING, `F2` INTEGER>, `ARRAY_ARRAY` ARRAY<ARRAY<STRING>>, `ARRAY_STRUCT` ARRAY<STRUCT<`F1` STRING>>, `ARRAY_MAP` ARRAY<MAP<STRING, INTEGER>>, `MAP_ARRAY` MAP<STRING, ARRAY<STRING>>, `MAP_MAP` MAP<STRING, MAP<STRING, INTEGER>>, `MAP_STRUCT` MAP<STRING, STRUCT<`F1` STRING>>>, "
            + "`TIMESTAMP` TIMESTAMP, "
            + "`DATE` DATE, "
            + "`TIME` TIME, "
            + "`HEAD` BYTES";

    // When
    final List<String> columnNames = RowUtil.colNamesFromSchema(schema);

    // Then
    assertThat(
            columnNames,
            contains(
                    "K",
                    "STR",
                    "LONG",
                    "DEC",
                    "BYTES_",
                    "ARRAY",
                    "MAP",
                    "STRUCT",
                    "COMPLEX",
                    "TIMESTAMP",
                    "DATE",
                    "TIME",
                    "HEAD"
            ));
  }

  @Test
  public void shouldGetColumnTypesFromSchema() {
    // Given
    final String schema = "`K` STRUCT<`F1` ARRAY<STRING>>, "
            + "`STR` STRING, "
            + "`LONG` BIGINT, "
            + "`DEC` DECIMAL(4, 2),"
            + "`BYTES_` BYTES, "
            + "`ARRAY` ARRAY<STRING>, "
            + "`MAP` MAP<STRING, STRING>, "
            + "`STRUCT` STRUCT<`F1` INTEGER>, "
            + "`COMPLEX` STRUCT<`DECIMAL` DECIMAL(2, 1), `STRUCT` STRUCT<`F1` STRING, `F2` INTEGER>, `ARRAY_ARRAY` ARRAY<ARRAY<STRING>>, `ARRAY_STRUCT` ARRAY<STRUCT<`F1` STRING>>, `ARRAY_MAP` ARRAY<MAP<STRING, INTEGER>>, `MAP_ARRAY` MAP<STRING, ARRAY<STRING>>, `MAP_MAP` MAP<STRING, MAP<STRING, INTEGER>>, `MAP_STRUCT` MAP<STRING, STRUCT<`F1` STRING>>>, "
            + "`TIMESTAMP` TIMESTAMP, "
            + "`DATE` DATE, "
            + "`TIME` TIME, "
            + "`HEAD` BYTES";

    // When
    final List<String> columnTypes = RowUtil.colTypesFromSchema(schema);

    // Then
    assertThat(
            columnTypes,
            contains(
                    "STRUCT<F1 ARRAY<STRING>>",
                    "STRING",
                    "BIGINT",
                    "DECIMAL(4, 2)",
                    "BYTES",
                    "ARRAY<STRING>",
                    "MAP<STRING, STRING>",
                    "STRUCT<F1 INTEGER>",
                    "STRUCT<DECIMAL DECIMAL(2, 1), STRUCT STRUCT<F1 STRING, F2 INTEGER>, ARRAY_ARRAY ARRAY<ARRAY<STRING>>, ARRAY_STRUCT ARRAY<STRUCT<F1 STRING>>, ARRAY_MAP ARRAY<MAP<STRING, INTEGER>>, MAP_ARRAY MAP<STRING, ARRAY<STRING>>, MAP_MAP MAP<STRING, MAP<STRING, INTEGER>>, MAP_STRUCT MAP<STRING, STRUCT<F1 STRING>>>",
                    "TIMESTAMP",
                    "DATE",
                    "TIME",
                    "BYTES"
            ));
  }
}