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

package io.confluent.ksql.schema.ksql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import org.junit.Test;

public class SqlBooleansTest {

  @Test
  public void shouldParseTrueAsTrue() {
    assertThat(SqlBooleans.parseBoolean("tRue"), is(true));
    assertThat(SqlBooleans.parseBoolean("trU"), is(true));
    assertThat(SqlBooleans.parseBoolean("tr"), is(true));
    assertThat(SqlBooleans.parseBoolean("T"), is(true));
    assertThat(SqlBooleans.parseBoolean("t"), is(true));
  }

  @Test
  public void shouldParseYesAsTrue() {
    assertThat(SqlBooleans.parseBoolean("YeS"), is(true));
    assertThat(SqlBooleans.parseBoolean("yE"), is(true));
    assertThat(SqlBooleans.parseBoolean("Y"), is(true));
  }

  @Test
  public void shouldParseAnythingElseAsFalse() {
    assertThat(SqlBooleans.parseBoolean(""), is(false));
    assertThat(SqlBooleans.parseBoolean(" true "), is(false));
    assertThat(SqlBooleans.parseBoolean("yes "), is(false));
    assertThat(SqlBooleans.parseBoolean("false"), is(false));
    assertThat(SqlBooleans.parseBoolean("what ever"), is(false));
  }

  @Test
  public void shouldParseExactTrueAsTrue() {
    assertThat(SqlBooleans.parseBooleanExact("trU"), is(Optional.of(true)));
  }

  @Test
  public void shouldParseExactYesAsTrue() {
    assertThat(SqlBooleans.parseBooleanExact("Ye"), is(Optional.of(true)));
  }

  @Test
  public void shouldParseExactFalseAsFalse() {
    assertThat(SqlBooleans.parseBooleanExact("fAlSe"), is(Optional.of(false)));
    assertThat(SqlBooleans.parseBooleanExact("FaLs"), is(Optional.of(false)));
    assertThat(SqlBooleans.parseBooleanExact("faL"), is(Optional.of(false)));
    assertThat(SqlBooleans.parseBooleanExact("FA"), is(Optional.of(false)));
    assertThat(SqlBooleans.parseBooleanExact("f"), is(Optional.of(false)));
  }

  @Test
  public void shouldParseExactNoAsFalse() {
    assertThat(SqlBooleans.parseBooleanExact("nO"), is(Optional.of(false)));
    assertThat(SqlBooleans.parseBooleanExact("N"), is(Optional.of(false)));
  }

  @Test
  public void shouldParseExactEverythingElseAsEmpty() {
    assertThat(SqlBooleans.parseBooleanExact("No "), is(Optional.empty()));
    assertThat(SqlBooleans.parseBooleanExact(" true"), is(Optional.empty()));
    assertThat(SqlBooleans.parseBooleanExact(" f "), is(Optional.empty()));
    assertThat(SqlBooleans.parseBooleanExact("/tfalse"), is(Optional.empty()));
    assertThat(SqlBooleans.parseBooleanExact("what ever"), is(Optional.empty()));
  }
}