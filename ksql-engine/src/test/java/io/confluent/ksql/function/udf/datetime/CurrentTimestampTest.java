/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.ksql.function.KsqlFunctionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Timestamp;
import java.time.ZoneId;

import static org.junit.Assert.assertTrue;

public class CurrentTimestampTest {

  private CurrentTimestamp udf;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    udf = new CurrentTimestamp();
  }

  @Test
  public void shouldGetTheCurrentTimestamp() {
    final long before = new Timestamp(System.currentTimeMillis()).getTime();
    final long result = udf.currentTimestamp();
    final long after = new Timestamp(System.currentTimeMillis()).getTime();

    assertTrue(before <= result && result <= after);
  }

  @Test
  public void shouldGetTheCurrentTimestampWithTimeZone() {
    final String timeZone = "UTC";

    final Timestamp before = new Timestamp(System.currentTimeMillis());
    before.toLocalDateTime().atZone(ZoneId.of(timeZone));

    final long result = udf.currentTimestamp("UTC");

    final Timestamp after = new Timestamp(System.currentTimeMillis());
    after.toLocalDateTime().atZone(ZoneId.of(timeZone));

    assertTrue(before.getTime() <= result && result <= after.getTime());
  }

  @Test
  public void shouldThrowIfInvalidTimeZone() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("Unknown time-zone ID: PST");
    udf.currentTimestamp("PST");
  }

}
