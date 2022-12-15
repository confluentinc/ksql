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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.util.KsqlException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import org.junit.Test;

public class SqlTimeTypesTest {
  @Test
  public void shouldParseTimestamp() {
    assertThat(SqlTimeTypes.parseTimestamp("2019-03-17T10:00:00"), is(new Timestamp(1552816800000L)));
    assertThat(SqlTimeTypes.parseTimestamp("2019-03-17T03:00-0700"), is(new Timestamp(1552816800000L)));
  }

  @Test
  public void shouldNotParseTimestamp() {
    assertThrows(KsqlException.class, () -> SqlTimeTypes.parseTimestamp("abc"));
    assertThrows(KsqlException.class, () -> SqlTimeTypes.parseTimestamp("2019-03-17 03:00"));
  }

  @Test
  public void shouldFormatTimestamp() {
    assertThat(SqlTimeTypes.formatTimestamp(new Timestamp(1552816800000L)), is("2019-03-17T10:00:00.000"));
  }

  @Test
  public void shouldParseTime() {
    assertThat(SqlTimeTypes.parseTime("10:00:00"), is(new Time(36000000)));
    assertThat(SqlTimeTypes.parseTime("10:00"), is(new Time(36000000)));
    assertThat(SqlTimeTypes.parseTime("10:00:00.001"), is(new Time(36000001)));
  }

  @Test
  public void shouldNotParseTime() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> SqlTimeTypes.parseTime("foo")
    );

    // Then
    assertThat(e.getMessage(), containsString(
        "Required format is: \"HH:mm:ss.SSS\""));
  }

  @Test
  public void shouldFormatTime() {
    assertThat(SqlTimeTypes.formatTime(new Time(1000)), is("00:00:01"));
    assertThat(SqlTimeTypes.formatTime(new Time(1005)), is("00:00:01"));
  }

  @Test
  public void shouldParseDate() {
    assertThat(SqlTimeTypes.parseDate("1990"), is(new Date(631152000000L)));
    assertThat(SqlTimeTypes.parseDate("1990-01"), is(new Date(631152000000L)));
    assertThat(SqlTimeTypes.parseDate("1990-01-01"), is(new Date(631152000000L)));
  }

  @Test
  public void shouldNotParseDate() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> SqlTimeTypes.parseDate("foo")
    );

    // Then
    assertThat(e.getMessage(), containsString(
        "Required format is: \"yyyy-MM-dd\""));
  }

  @Test
  public void shouldFormatDate() {
    assertThat(SqlTimeTypes.formatDate(new Date(864000000)), is("1970-01-11"));
    assertThat(SqlTimeTypes.formatDate(new Date(864000000 - 10)), is("1970-01-10"));
  }
}
