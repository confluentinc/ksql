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
    // When:
    final Object result = udf.convertTz(Timestamp.valueOf("2000-01-01 00:00:00"), "+0200", "+0500");

    // Then:
    assertThat(result, is(Timestamp.valueOf("2000-01-01 03:00:00")));
  }

  @Test
  public void shouldConvertTimezoneWithName() {
    // When:
    final Object result = udf.convertTz(Timestamp.valueOf("2000-01-01 00:00:00"), "America/Los_Angeles", "America/New_York");

    // Then:
    assertThat(result, is(Timestamp.valueOf("2000-01-01 03:00:00")));
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
  public void shouldReturnNullForNullTimestamp() {
    // When:
    final Object result = udf.convertTz(null, "America/Los_Angeles", "America/New_York");

    // Then:
    assertNull(result);
  }

  @Test
  public void shouldReturnNullForNullFromTimeZone() {
    // When:
    final Object result = udf.convertTz(Timestamp.valueOf("2000-01-01 00:00:00"), null, "America/New_York");

    // Then:
    assertNull(result);
  }

  @Test
  public void shouldReturnNullForNullToTimeZone() {
    // When:
    final Object result = udf.convertTz(Timestamp.valueOf("2000-01-01 00:00:00"), "America/Los_Angeles", null);

    // Then:
    assertNull(result);
  }
}
