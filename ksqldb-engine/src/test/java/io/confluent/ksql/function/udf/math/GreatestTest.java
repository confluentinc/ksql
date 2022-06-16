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

public class GreatestTest {

  private Greatest greatestUDF;
  private ToBytes toBytesUDF;

  @Before
  public void setUp() {
    greatestUDF = new Greatest();
    toBytesUDF = new ToBytes();
  }

  @Test
  public void shouldWorkWithoutImplicitCasting() {
    assertThat(greatestUDF.greatest(0, 1, -1, 2, -2), is(2));
    assertThat(greatestUDF.greatest(0L, 1L, -1L, 2L, -2L), is(2L));
    assertThat(greatestUDF.greatest(0D, .1, -.1, .2, -.2), is(.2));
    assertThat(greatestUDF.greatest(new BigDecimal("0"), new BigDecimal(".1"), new BigDecimal("-.1"), new BigDecimal(".2"), new BigDecimal("-.2")), is(new BigDecimal(".2")));
    assertThat(greatestUDF.greatest("apple", "banana", "bzzz"), is("bzzz"));
    assertThat(greatestUDF.greatest(toBytes("apple"), toBytes("banana"), toBytes("aardvark")), is(toBytes("banana")));
    assertThat(greatestUDF.greatest(new Date(10), new Date(0), new Date(-5), new Date(100), new Date(2)), is(new Date(100)));
    assertThat(greatestUDF.greatest(new Time(10), new Time(0), new Time(-5), new Time(100), new Time(2)), is(new Time(100)));
    assertThat(greatestUDF.greatest(new Timestamp(10), new Timestamp(0), new Timestamp(-5), new Timestamp(100), new Timestamp(2)), is(new Timestamp(100)));
  }

  @Test
  public void shouldHandleAllNullColumns() {
    assertThat(greatestUDF.greatest((Integer) null, null, null), is(nullValue()));
    assertThat(greatestUDF.greatest((Double) null, null, null), is(nullValue()));
    assertThat(greatestUDF.greatest((Long) null, null, null), is(nullValue()));
    assertThat(greatestUDF.greatest((BigDecimal) null, null, null), is(nullValue()));
    assertThat(greatestUDF.greatest((String) null, null, null), is(nullValue()));
    assertThat(greatestUDF.greatest((ByteBuffer) null, null, null), is(nullValue()));
    assertThat(greatestUDF.greatest((Date) null, null, null), is(nullValue()));
    assertThat(greatestUDF.greatest((Time) null, null, null), is(nullValue()));
    assertThat(greatestUDF.greatest((Timestamp) null, null, null), is(nullValue()));
  }

  @Test
  public void shouldHandleNullArrays() {
    assertThat(greatestUDF.greatest(null, (Integer[]) null), is(nullValue()));
    assertThat(greatestUDF.greatest(null, (Double[]) null), is(nullValue()));
    assertThat(greatestUDF.greatest(null, (Long[]) null), is(nullValue()));
    assertThat(greatestUDF.greatest(null, (BigDecimal[]) null), is(nullValue()));
    assertThat(greatestUDF.greatest(null, (String[]) null), is(nullValue()));
    assertThat(greatestUDF.greatest(null, (ByteBuffer[]) null), is(nullValue()));
    assertThat(greatestUDF.greatest(null, (Date[]) null), is(nullValue()));
    assertThat(greatestUDF.greatest(null, (Time[]) null), is(nullValue()));
    assertThat(greatestUDF.greatest(null, (Timestamp[]) null), is(nullValue()));
  }

  @Test
  public void shouldHandleSomeNullColumns() {
    assertThat(greatestUDF.greatest(null, 27, null, 39, -49, -11, 68, 32, null, 101), is(101));
    assertThat(greatestUDF.greatest(null, null, 39D, -49.01, -11.98, 68.1, .32, null, 101D), is(101D));
    assertThat(greatestUDF.greatest(null, 272038202439L, null, 39L, -4923740932490L, -11L, 68L, 32L, null, 101L), is(272038202439L));
    assertThat(greatestUDF.greatest(null, new BigDecimal("27"), null, new BigDecimal("-49")), is(new BigDecimal("27")));
    assertThat(greatestUDF.greatest(null, "apple", null, "banana", "kumquat", "aardvark", null), is("kumquat"));
    assertThat(greatestUDF.greatest(null, toBytes("apple"), null, toBytes("banana"), toBytes("kumquat"), toBytes("aardvark"), null), is(toBytes("kumquat")));
    assertThat(greatestUDF.greatest(null, new Date(10), null, new Date(0), new Date(-5), new Date(100), null, new Date(2)), is(new Date(100)));
    assertThat(greatestUDF.greatest(null, new Time(10), null, new Time(0), new Time(-5), new Time(100), null, new Time(2)), is(new Time(100)));
    assertThat(greatestUDF.greatest(null, new Timestamp(10), null, new Timestamp(0), new Timestamp(-5), new Timestamp(100), null, new Timestamp(2)), is(new Timestamp(100)));
  }

  private ByteBuffer toBytes(final String val) {
    return toBytesUDF.toBytes(val, BytesUtils.Encoding.ASCII.toString());
  }

}