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

import java.sql.Timestamp;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class UnixTimestampTest {

  private UnixTimestamp udf;

  @Before
  public void setUp() {
    udf = new UnixTimestamp();
  }

  @Test
  public void shouldGetTheUnixTimestamp() {
    // Given:
    final long before = System.currentTimeMillis();

    // When:
    final long result = udf.unixTimestamp();
    final long after = System.currentTimeMillis();

    // Then:
    assertTrue(before <= result && result <= after);
  }

  @Test
  public void shouldReturnMilliseconds() {
    // When:
    final long result = udf.unixTimestamp(new Timestamp(100L));

    // Then:
    assertThat(result, is(100L));
  }

  @Test
  public void shouldReturnNull() {
    // When:
    final Long result = udf.unixTimestamp(null);

    // Then:
    assertNull(result);
  }
}
