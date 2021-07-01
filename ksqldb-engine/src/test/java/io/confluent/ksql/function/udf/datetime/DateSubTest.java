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

import java.sql.Date;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class DateSubTest {
  private DateSub udf;

  @Before
  public void setUp() {
    udf = new DateSub();
  }

  @Test
  public void shouldAddToDate() {
    assertThat(udf.dateSub(TimeUnit.DAYS, 11, new Date(86400000)), is(new Date(-864000000)));
    assertThat(udf.dateSub(TimeUnit.DAYS, -1, new Date(86400000)), is(new Date(172800000)));
    assertThat(udf.dateSub(TimeUnit.SECONDS, 5, new Date(86400000)), is(new Date(0)));
  }

  @Test
  public void handleNulls() {
    assertNull(udf.dateSub(TimeUnit.DAYS, -300, null));
    assertNull(udf.dateSub(null, 54, new Date(864000000)));
    assertNull(udf.dateSub(TimeUnit.DAYS, null, new Date(864000000)));
  }
}
