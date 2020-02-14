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

package io.confluent.ksql.api.plugin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlValueCoercer;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class KeyValueExtractorTest {

  private static final SqlValueCoercer SQL_VALUE_COERCER = DefaultSqlValueCoercer.INSTANCE;

  @Test
  public void shouldExtractIntKey() {
    shouldExtractKey(10, 10, SqlTypes.INTEGER);
  }

  @Test
  public void shouldExtractLongKey() {
    shouldExtractKey(10L, 10L, SqlTypes.BIGINT);
  }

  @Test
  public void shouldExtractStringKey() {
    shouldExtractKey("10", "10", SqlTypes.STRING);
  }

  @Test
  public void shouldExtractBooleanKey() {
    shouldExtractKey(true, true, SqlTypes.BOOLEAN);
  }

  @Test
  public void shouldExtractDoubleKey() {
    shouldExtractKey(1.5d, 1.5d, SqlTypes.DOUBLE);
  }

  @Test
  public void shouldExtractIntKeyUpcastToBigInt() {
    shouldExtractKey(10, 10L, SqlTypes.BIGINT);
  }

  @Test
  public void shouldExtractDoubleKeyUpcastToDecimal() {
    shouldExtractKey(1.5d, new BigDecimal("1.50"), SqlTypes.decimal(10, 2));
  }

  @Test
  public void shouldExtractIntKeyUpcastToDecimal() {
    shouldExtractKey(10, new BigDecimal("10.00"), SqlTypes.decimal(10, 2));
  }

  @Test
  public void shouldExtractLongKeyUpcastToDecimal() {
    shouldExtractKey(10L, new BigDecimal("10.00"), SqlTypes.decimal(10, 2));
  }

  @Test
  public void shouldExtractValues() {
    JsonObject jsonObject =
        new JsonObject().put("mykey", 10)
            .put("stringfield", "stringval")
            .put("booleanfield", true)
            .put("doublefield", 1.7d)
            .put("intfield", 11)
            .put("bigintfield", 13L)
            .put("doubledecimalfield", 100.1d)
            .put("stringdecimalfield", "1234.5678")
            .put("intdecimalfield", "10");

    LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("mykey"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("stringfield"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("booleanfield"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("doublefield"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("intfield"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("bigintfield"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("doubledecimalfield"), SqlTypes.decimal(10, 2))
        .valueColumn(ColumnName.of("stringdecimalfield"), SqlTypes.decimal(10, 2))
        .valueColumn(ColumnName.of("intdecimalfield"), SqlTypes.decimal(10, 2))
        .build();

    final GenericRow row = KeyValueExtractor.extractValues(jsonObject, schema, SQL_VALUE_COERCER);
    assertThat(row, is(notNullValue()));
    assertThat(row.values(), hasSize(8));
    assertThat(row.get(0), is("stringval"));
    assertThat(row.get(1), is(true));
    assertThat(row.get(2), is(1.7d));
    assertThat(row.get(3), is(11));
    assertThat(row.get(4), is(13L));
    assertThat(row.get(5), is(new BigDecimal("100.10")));
    assertThat(row.get(6), is(new BigDecimal("1234.57")));
    assertThat(row.get(7), is(new BigDecimal("10.00")));
  }

  private void shouldExtractKey(final Object keyValue, final Object expectedValue,
      final SqlType keyType) {
    JsonObject jsonObject =
        new JsonObject().put("mykey", keyValue)
            .put("foo", "blah");

    LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("mykey"), keyType)
        .valueColumn(ColumnName.of("foo"), SqlTypes.STRING)
        .build();

    final Struct key = KeyValueExtractor.extractKey(jsonObject, schema, SQL_VALUE_COERCER);
    assertThat(key, is(notNullValue()));
    assertThat(key.schema().fields(), hasSize(1));
    assertThat(key.get("mykey"), is(expectedValue));
  }

}
