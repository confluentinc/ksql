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

package io.confluent.ksql.api.client.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.util.RowUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class RowImplTest {

  private static final List<String> COLUMN_NAMES =
      ImmutableList.of("f_str", "f_int", "f_long", "f_double", "f_bool", "f_decimal", "f_array", "f_map", "f_struct", "f_null");
  private static final List<ColumnType> COLUMN_TYPES = RowUtil.columnTypesFromStrings(
      ImmutableList.of("STRING", "INTEGER", "BIGINT", "DOUBLE", "BOOLEAN", "DECIMAL", "ARRAY", "MAP", "STRUCT", "INTEGER"));
  private static final Map<String, Integer> COLUMN_NAME_TO_INDEX = RowUtil.valueToIndexMap(COLUMN_NAMES);
  private static final JsonArray VALUES = new JsonArray()
      .add("foo")
      .add(2)
      .add(1234L)
      .add(34.43)
      .add(false)
      .add(12.21) // server endpoint returns decimals as doubles
      .add(new JsonArray("[\"e1\",\"e2\"]"))
      .add(new JsonObject("{\"k1\":\"v1\",\"k2\":\"v2\"}"))
      .add(new JsonObject("{\"f1\":\"baz\",\"f2\":12}"))
      .addNull();

  private RowImpl row;

  @Before
  public void setUp() {
    row = new RowImpl(COLUMN_NAMES, COLUMN_TYPES, VALUES, COLUMN_NAME_TO_INDEX);
  }

  @Test
  public void shouldOneIndexColumnNames() {
    assertThat(row.getValue(1), is("foo"));
    assertThat(row.getValue(2), is(2));
    assertThat(row.getValue(3), is(1234L));
    assertThat(row.getValue(4), is(34.43));
    assertThat(row.getValue(5), is(false));
    assertThat(row.getValue(6), is(12.21));
    assertThat(row.getValue(7), is(new JsonArray("[\"e1\",\"e2\"]")));
    assertThat(row.getValue(8), is(new JsonObject("{\"k1\":\"v1\",\"k2\":\"v2\"}")));
    assertThat(row.getValue(9), is(new JsonObject("{\"f1\":\"baz\",\"f2\":12}")));
    assertThat(row.getValue(10), is(nullValue()));
  }

  @Test
  public void shouldGetString() {
    assertThat(row.getString("f_str"), is("foo"));
  }

  @Test
  public void shouldGetInt() {
    assertThat(row.getInt("f_int"), is(2));
  }

  @Test
  public void shouldGetLong() {
    assertThat(row.getLong("f_long"), is(1234L));
    assertThat(row.getLong("f_int"), is(2L));
  }

  @Test
  public void shouldGetDouble() {
    assertThat(row.getDouble("f_double"), is(34.43));
  }

  @Test
  public void shouldGetBoolean() {
    assertThat(row.getBoolean("f_bool"), is(false));
  }

  @Test
  public void shouldGetDecimal() {
    assertThat(row.getDecimal("f_decimal"), is(new BigDecimal("12.21")));
  }

  @Test
  public void shouldGetKsqlArray() {
    assertThat(row.getKsqlArray("f_array"), is(new KsqlArray(ImmutableList.of("e1", "e2"))));
  }

  @Test
  public void shouldGetKsqlObject() {
    assertThat(row.getKsqlObject("f_map"), is(new KsqlObject(ImmutableMap.of("k1", "v1", "k2", "v2"))));
    assertThat(row.getKsqlObject("f_struct"), is(new KsqlObject(ImmutableMap.of("f1", "baz", "f2", 12))));
  }

  @Test
  public void shouldReturnNull() {
    assertThat(row.isNull("f_null"), is(true));
    assertThat(row.isNull("f_bool"), is(false));
    assertThat(row.isNull("f_struct"), is(false));
  }
}