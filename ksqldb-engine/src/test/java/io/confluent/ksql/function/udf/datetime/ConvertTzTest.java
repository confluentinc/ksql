/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.function.udf.datetime;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.function.KsqlFunctionException;
import java.sql.Time;
import java.sql.Timestamp;
import org.junit.Before;
import org.junit.Test;

public class ConvertTzTest {
  private ConvertTz udf;

  @Before
  public void setUp() {
    udf = new ConvertTz();
  }

  @Test
  public void shouldConvertTimezoneWithOffset() {
    assertThat(udf.convertTz(Timestamp.valueOf("2000-01-01 00:00:00"), "+0200", "+0500"),
        is(Timestamp.valueOf("2000-01-01 03:00:00")));
    assertThat(udf.convertTz(Time.valueOf("00:00:00"), "+0200", "+0500"),
        is(Time.valueOf("03:00:00")));
  }

  @Test
  public void shouldConvertTimezoneWithName() {
    // When:
    assertThat(udf.convertTz(Timestamp.valueOf("2000-01-01 00:00:00"), "America/Los_Angeles", "America/New_York"),
        is(Timestamp.valueOf("2000-01-01 03:00:00")));
    assertThat(udf.convertTz(Time.valueOf("00:00:00"), "America/Los_Angeles", "America/New_York"),
        is(Time.valueOf("03:00:00")));
  }

  @Test
  public void shouldThrowOnInvalidTimezone() {
    // When:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.convertTz(Timestamp.valueOf("2000-01-01 00:00:00"), "wow", "amazing")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid time zone"));
  }

  @Test
  public void shouldReturnNull() {
    assertNull(udf.convertTz((Timestamp) null, "America/Los_Angeles", "America/New_York"));
    assertNull(udf.convertTz((Time) null, "America/Los_Angeles", "America/New_York"));
  }

  @Test
  public void shouldReturnTimeWhenConversionGoesToAnotherDay() {
    assertThat(udf.convertTz(new Time(82800000), "America/Los_Angeles", "America/New_York"),
        is(new Time(7200000)));
    assertThat(udf.convertTz(new Time(7200000), "America/New_York", "America/Los_Angeles"),
        is(new Time(82800000)));
  }
}
