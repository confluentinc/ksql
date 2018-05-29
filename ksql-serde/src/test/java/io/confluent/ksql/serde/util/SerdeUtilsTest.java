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

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

import org.junit.Test;

public class SerdeUtilsTest {

  @Test
  public void shouldConvertToBooleanCorrectly() {
    Boolean b = SerdeUtils.toBoolean(new Boolean(true));
    assertThat(b, equalTo(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonBooleanToBoolean() {
    Boolean b = SerdeUtils.toBoolean(new Integer(1));
  }

  @Test
  public void shouldConvertToIntCorrectly() {
    Integer i = SerdeUtils.toInteger(new Integer(1));
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertLongToIntCorrectly() {
    Integer i = SerdeUtils.toInteger(new Long(1L));
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertDoubleToIntCorrectly() {
    Integer i = SerdeUtils.toInteger(new Double(1.0));
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertStringToIntCorrectly() {
    Integer i = SerdeUtils.toInteger("1");
    assertThat(i, equalTo(1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotConvertIncorrectStringToInt() {
    Integer i = SerdeUtils.toInteger("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonIntegerToIntegr() {
    Object i = SerdeUtils.toInteger(new Boolean(true));
  }

  @Test
  public void shouldConvertToLongCorrectly() {
    Long l = SerdeUtils.toLong(new Long(1L));
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertIntToLongCorrectly() {
    Long l = SerdeUtils.toLong(new Integer(1));
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertDoubleToLongCorrectly() {
    Long l = SerdeUtils.toLong(new Double(1.0));
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertStringToLongCorrectly() {
    Long l = SerdeUtils.toLong("1");
    assertThat(l, equalTo(1L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotConvertIncorrectStringToLong() {
    Long l = SerdeUtils.toLong("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleLong() {
    Object i = SerdeUtils.toInteger(new Boolean(true));
  }


  @Test
  public void shouldConvertToDoubleCorrectly() {
    Double d = SerdeUtils.toDouble(new Double(1.0));
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertIntToDoubleCorrectly() {
    Double d = SerdeUtils.toDouble(new Integer(1));
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertLongToDoubleCorrectly() {
    Double d = SerdeUtils.toDouble(new Long(1L));
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertStringToDoubleCorrectly() {
    Double d = SerdeUtils.toDouble("1.0");
    assertThat(d, equalTo(1.0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotConvertIncorrectStringToDouble() {
    Double d = SerdeUtils.toDouble("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleDouble() {
    Object i = SerdeUtils.toDouble(new Boolean(true));
  }


}
