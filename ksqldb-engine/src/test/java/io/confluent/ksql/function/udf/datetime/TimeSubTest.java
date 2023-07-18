/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.function.udf.datetime;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class TimeSubTest {
  private TimeSub udf;

  @Before
  public void setUp() {
    udf = new TimeSub();
  }

  @Test
  public void shouldAddToTime() {
    // When:
    assertThat(udf.timeSub(TimeUnit.MILLISECONDS, 50, new Time(1000)).getTime(), is(950L));
    assertThat(udf.timeSub(TimeUnit.DAYS, 2, new Time(1000)).getTime(), is(1000L));
    assertThat(udf.timeSub(TimeUnit.DAYS, -2, new Time(1000)).getTime(), is(1000L));
    assertThat(udf.timeSub(TimeUnit.MINUTES, -1, new Time(60000)).getTime(), is(120000L));
  }

  @Test
  public void handleNullTime() {
    assertNull(udf.timeSub(TimeUnit.MILLISECONDS, -300, null));
  }
}