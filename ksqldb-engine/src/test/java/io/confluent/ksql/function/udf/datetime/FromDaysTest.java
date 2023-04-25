/*
 * Copyright 2021 Confluent Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Date;
import org.junit.Before;
import org.junit.Test;

public class FromDaysTest {
  private FromDays udf;

  @Before
  public void setUp() {
    udf = new FromDays();
  }

  @Test
  public void shouldConvertToTimestamp() {
    assertThat(udf.fromDays(50), is(new Date(4320000000L)));
    assertThat(udf.fromDays(-50), is(new Date(-4320000000L)));
  }
}
