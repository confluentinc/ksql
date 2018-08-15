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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class SerdeUtilsTest {

  @Test
  public void shouldConvertToBooleanCorrectly() {
    final Boolean b = SerdeUtils.toBoolean(true);
    assertThat(b, equalTo(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonBooleanToBoolean() {
    final Boolean b = SerdeUtils.toBoolean(1);
  }

  @Test
  public void shouldConvertToIntCorrectly() {
    final Integer i = SerdeUtils.toInteger(1);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertLongToIntCorrectly() {
    final Integer i = SerdeUtils.toInteger(1L);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertDoubleToIntCorrectly() {
    final Integer i = SerdeUtils.toInteger(1.0);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertStringToIntCorrectly() {
    final Integer i = SerdeUtils.toInteger("1");
    assertThat(i, equalTo(1));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToInt() {
    final Integer i = SerdeUtils.toInteger("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonIntegerToIntegr() {
    final Object i = SerdeUtils.toInteger(true);
  }

  @Test
  public void shouldConvertToLongCorrectly() {
    final Long l = SerdeUtils.toLong(1L);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertIntToLongCorrectly() {
    final Long l = SerdeUtils.toLong(1);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertDoubleToLongCorrectly() {
    final Long l = SerdeUtils.toLong(1.0);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertStringToLongCorrectly() {
    final Long l = SerdeUtils.toLong("1");
    assertThat(l, equalTo(1L));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToLong() {
    final Long l = SerdeUtils.toLong("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleLong() {
    final Object i = SerdeUtils.toInteger(true);
  }


  @Test
  public void shouldConvertToDoubleCorrectly() {
    final Double d = SerdeUtils.toDouble(1.0);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertIntToDoubleCorrectly() {
    final Double d = SerdeUtils.toDouble(1);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertLongToDoubleCorrectly() {
    final Double d = SerdeUtils.toDouble(1L);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertStringToDoubleCorrectly() {
    final Double d = SerdeUtils.toDouble("1.0");
    assertThat(d, equalTo(1.0));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToDouble() {
    final Double d = SerdeUtils.toDouble("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleDouble() {
    final Object i = SerdeUtils.toDouble(true);
  }
}
