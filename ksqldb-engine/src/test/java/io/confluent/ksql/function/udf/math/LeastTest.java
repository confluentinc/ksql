/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.math;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import io.confluent.ksql.function.udf.string.ToBytes;
import io.confluent.ksql.util.BytesUtils;
import org.junit.Before;
import org.junit.Test;

public class LeastTest {

  private Least leastUDF;
  private ToBytes toBytesUDF;

  @Before
  public void setUp() {
    leastUDF = new Least();
    toBytesUDF = new ToBytes();
  }

  @Test
  public void shouldWorkWithoutImplicitCasting() {
    assertThat(leastUDF.least(0, 1, -1, 2, -2), is(-2));
    assertThat(leastUDF.least(0L, 1L, -1L, 2L, -2L), is(-2L));
    assertThat(leastUDF.least(0D, .1, -.1, .2, -.2), is(-.2));
    assertThat(leastUDF.least(new BigDecimal("0"), new BigDecimal(".1"), new BigDecimal("-.1"), new BigDecimal(".2"), new BigDecimal("-.2")), is(new BigDecimal("-.2")));
    assertThat(leastUDF.least("apple", "banana", "aardvark"), is("aardvark"));
    assertThat(leastUDF.least(toBytes("apple"), toBytes("banana"), toBytes("aardvark")), is(toBytes("aardvark")));
    assertThat(leastUDF.least(new Date(10), new Date(0), new Date(-5), new Date(100), new Date(2)), is(new Date(-5)));
    assertThat(leastUDF.least(new Time(10), new Time(0), new Time(-5), new Time(100), new Time(2)), is(new Time(-5)));
    assertThat(leastUDF.least(new Timestamp(10), new Timestamp(0), new Timestamp(-5), new Timestamp(100), new Timestamp(2)), is(new Timestamp(-5)));
  }

  @Test
  public void shouldHandleAllNullColumns() {
    assertThat(leastUDF.least((Integer) null, null, null), is(nullValue()));
    assertThat(leastUDF.least((Double) null, null, null), is(nullValue()));
    assertThat(leastUDF.least((Long) null, null, null), is(nullValue()));
    assertThat(leastUDF.least((BigDecimal) null, null, null), is(nullValue()));
    assertThat(leastUDF.least((String) null, null, null), is(nullValue()));
    assertThat(leastUDF.least((ByteBuffer) null, null, null), is(nullValue()));
    assertThat(leastUDF.least((Date) null, null, null), is(nullValue()));
    assertThat(leastUDF.least((Time) null, null, null), is(nullValue()));
    assertThat(leastUDF.least((Timestamp) null, null, null), is(nullValue()));
  }
  
  @Test
  public void shouldHandleNullArrays() {
    assertThat(leastUDF.least(null, (Integer[]) null), is(nullValue()));
    assertThat(leastUDF.least(null, (Double[]) null), is(nullValue()));
    assertThat(leastUDF.least(null, (Long[]) null), is(nullValue()));
    assertThat(leastUDF.least(null, (BigDecimal[]) null), is(nullValue()));
    assertThat(leastUDF.least(null, (String[]) null), is(nullValue()));
    assertThat(leastUDF.least(null, (ByteBuffer[]) null), is(nullValue()));
    assertThat(leastUDF.least(null, (Date[]) null), is(nullValue()));
    assertThat(leastUDF.least(null, (Time[]) null), is(nullValue()));
    assertThat(leastUDF.least(null, (Timestamp[]) null), is(nullValue()));
  }

  @Test
  public void shouldHandleSomeNullColumns() {
    assertThat(leastUDF.least(null, 27, null, 39, -49, -11, 68, 32, null, 101), is(-49));
    assertThat(leastUDF.least(null, null, 39d, -49.01, -11.98, 68.1, .32, null, 101d), is(-49.01));
    assertThat(leastUDF.least(null, 272038202439L, null, 39L, -4923740932490L, -11L, 68L, 32L, null, 101L), is(-4923740932490L));
    assertThat(leastUDF.least(null, new BigDecimal("27"), null, new BigDecimal("-49")), is(new BigDecimal("-49")));
    assertThat(leastUDF.least(null, "apple", null, "banana", "kumquat", "aardvark", null), is("aardvark"));
    assertThat(leastUDF.least(null, toBytes("apple"), null, toBytes("banana"), toBytes("kumquat"), toBytes("aardvark"), null), is(toBytes("aardvark")));
    assertThat(leastUDF.least(null, new Date(10), null, new Date(0), new Date(-5), new Date(100), null, new Date(2)), is(new Date(-5)));
    assertThat(leastUDF.least(null, new Time(10), null, new Time(0), new Time(-5), new Time(100), null, new Time(2)), is(new Time(-5)));
    assertThat(leastUDF.least(null, new Timestamp(10), null, new Timestamp(0), new Timestamp(-5), new Timestamp(100), null, new Timestamp(2)), is(new Timestamp(-5)));
  }

  private ByteBuffer toBytes(final String val) {
    return toBytesUDF.toBytes(val, BytesUtils.Encoding.ASCII.toString());
  }

}