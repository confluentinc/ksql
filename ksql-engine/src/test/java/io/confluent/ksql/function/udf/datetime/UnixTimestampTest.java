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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Timestamp;

import static org.junit.Assert.assertTrue;

public class UnixTimestampTest {

  private UnixTimestamp udf;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
}
