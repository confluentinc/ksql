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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.api.client.util.RowUtil;
import io.vertx.core.json.JsonArray;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class RowImplTest {

  private static final List<String> COLUMN_NAMES = ImmutableList.of("f_str", "f_int", "f_long", "f_double", "f_bool");
  private static final List<String> COLUMN_TYPES = ImmutableList.of("STRING", "INTEGER", "BIGINT", "DOUBLE", "BOOLEAN");
  private static final Map<String, Integer> COLUMN_NAME_TO_INDEX = RowUtil.valueToIndexMap(COLUMN_NAMES);
  private static final JsonArray VALUES = new JsonArray(ImmutableList.of("foo", 2, 1234L, 34.43, false));

  private RowImpl row;

  @Before
  public void setUp() {
    row = new RowImpl(COLUMN_NAMES, COLUMN_TYPES, VALUES, COLUMN_NAME_TO_INDEX);
  }

  @Test
  public void shouldOneIndexColumnNames() {
    assertThat(row.getObject(1), is("foo"));
    assertThat(row.getObject(2), is(2));
    assertThat(row.getObject(3), is(1234L));
    assertThat(row.getObject(4), is(34.43));
    assertThat(row.getObject(5), is(false));
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
}