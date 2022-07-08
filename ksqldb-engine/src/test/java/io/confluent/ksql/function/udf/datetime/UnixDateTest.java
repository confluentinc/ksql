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

import java.sql.Date;
import org.junit.Before;
import org.junit.Test;
import java.time.LocalDate;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class UnixDateTest {

  private UnixDate udf;

  @Before
  public void setUp() {
    udf = new UnixDate();
  }

  @Test
  public void shouldGetTheUnixDate() {
    // Given:
    final int now = ((int) LocalDate.now().toEpochDay());

    // When:
    final int result = udf.unixDate();

    // Then:
    assertEquals(now, result);
  }

  @Test
  public void shouldReturnDays() {
    // When:
    final int result = udf.unixDate(new Date(864000000));

    // Then:
    assertThat(result, is(10));
  }

  @Test
  public void shouldReturnNull() {
    // When:
    final Integer result = udf.unixDate(null);

    // Then:
    assertNull(result);
  }

}
