/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.schema.ksql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.util.KsqlException;
import java.sql.Timestamp;
import org.junit.Test;

public class SqlTimestampsTest {
  @Test
  public void shouldParseTimestamp() {
    assertThat(SqlTimestamps.parseTimestamp("2019-03-17T10:00:00"), is(new Timestamp(1552816800000L)));
    assertThat(SqlTimestamps.parseTimestamp("2019-03-17T03:00-0700"), is(new Timestamp(1552816800000L)));
  }

  @Test
  public void shouldNotParseTimestamp() {
    assertThrows(KsqlException.class, () -> SqlTimestamps.parseTimestamp("abc"));
    assertThrows(KsqlException.class, () -> SqlTimestamps.parseTimestamp("2019-03-17 03:00"));
  }

  @Test
  public void shouldFormatTimestamp() {
    assertThat(SqlTimestamps.formatTimestamp(new Timestamp(1552816800000L)), is("2019-03-17T10:00:00.000"));
  }
}
