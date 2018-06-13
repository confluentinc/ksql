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

package io.confluent.ksql.serde.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class SerdeUtilsTest {

  @Test
  public void shouldConvertToBooleanCorrectly() {
    Boolean b = SerdeUtils.toBoolean(true);
    assertThat(b, equalTo(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonBooleanToBoolean() {
    Boolean b = SerdeUtils.toBoolean(1);
  }

  @Test
  public void shouldConvertToIntCorrectly() {
    Integer i = SerdeUtils.toInteger(1);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertLongToIntCorrectly() {
    Integer i = SerdeUtils.toInteger(1L);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertDoubleToIntCorrectly() {
    Integer i = SerdeUtils.toInteger(1.0);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertStringToIntCorrectly() {
    Integer i = SerdeUtils.toInteger("1");
    assertThat(i, equalTo(1));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToInt() {
    Integer i = SerdeUtils.toInteger("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonIntegerToIntegr() {
    Object i = SerdeUtils.toInteger(true);
  }

  @Test
  public void shouldConvertToLongCorrectly() {
    Long l = SerdeUtils.toLong(1L);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertIntToLongCorrectly() {
    Long l = SerdeUtils.toLong(1);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertDoubleToLongCorrectly() {
    Long l = SerdeUtils.toLong(1.0);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertStringToLongCorrectly() {
    Long l = SerdeUtils.toLong("1");
    assertThat(l, equalTo(1L));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToLong() {
    Long l = SerdeUtils.toLong("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleLong() {
    Object i = SerdeUtils.toInteger(true);
  }


  @Test
  public void shouldConvertToDoubleCorrectly() {
    Double d = SerdeUtils.toDouble(1.0);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertIntToDoubleCorrectly() {
    Double d = SerdeUtils.toDouble(1);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertLongToDoubleCorrectly() {
    Double d = SerdeUtils.toDouble(1L);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertStringToDoubleCorrectly() {
    Double d = SerdeUtils.toDouble("1.0");
    assertThat(d, equalTo(1.0));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToDouble() {
    Double d = SerdeUtils.toDouble("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleDouble() {
    Object i = SerdeUtils.toDouble(true);
  }
}
