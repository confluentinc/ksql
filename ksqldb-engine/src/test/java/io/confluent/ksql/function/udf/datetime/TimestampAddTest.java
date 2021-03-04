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

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class TimestampAddTest {
  private TimestampAdd udf;

  @Before
  public void setUp() {
    udf = new TimestampAdd();
  }

  @Test
  public void addToTimestamp() {
    // When:
    final Timestamp result = udf.timestampAdd(TimeUnit.MILLISECONDS, 50, new Timestamp(100));

    // Then:
    final Timestamp expectedResult = new Timestamp(150);
    assertThat(result, is(expectedResult));
  }

  @Test
  public void addNegativeTimestamp() {
    // When:
    final Timestamp result = udf.timestampAdd(TimeUnit.MILLISECONDS, -300, new Timestamp(100));

    // Then:
    final Timestamp expectedResult = new Timestamp(-200);
    assertThat(result, is(expectedResult));
  }
}
