/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.parser;

import static io.confluent.ksql.parser.DurationParser.parse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import org.junit.Test;


public class DurationParserTest {

  @Test
  public void shouldThrowOnTooFewTokens() {
    // Then:
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> parse("10")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected 2 tokens, got: 1"));
  }

  @Test
  public void shouldThrowOnTooManyTokens() {
    // Then:
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> parse("10 Seconds Long")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected 2 tokens, got: 3"));
  }

  @Test
  public void shouldThrowOnNonNumericDuration() {
    // Then:
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> parse("10s Seconds")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Not numeric: '10s'"));
  }

  @Test
  public void shouldThrowOnUnknownTimeUnit() {
    // Then:
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> parse("10 Green_Bottles")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown time unit: 'GREEN_BOTTLES'"));
  }

  @Test
  public void shouldIncludeOriginalInExceptionMessage() {
    // Then:
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> parse("10 Green_Bottles")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid duration: '10 Green_Bottles'. "));
  }

  @Test
  public void shouldIncludeValidTimeUnitsInExceptionMessage() {
    // Then:
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> parse("10 Bananas")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Supported time units are: "
        + "NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS"));
  }

  @Test
  public void shouldSupportNanos() {
    assertThat(DurationParser.parse("27 NANOSECONDS"), is(Duration.ofNanos(27)));
  }

  @Test
  public void shouldSupportMillis() {
    assertThat(DurationParser.parse("756 MILLISECONDS"), is(Duration.ofMillis(756)));
  }

  @Test
  public void shouldParseSingular() {
    assertThat(DurationParser.parse("1 Second"), is(Duration.ofSeconds(1)));
  }

  @Test
  public void shouldParseMultiple() {
    assertThat(DurationParser.parse("10 minutes"), is(Duration.ofMinutes(10)));
  }

  @Test
  public void shouldSupportHours() {
    assertThat(DurationParser.parse("12 HOURs"), is(Duration.ofHours(12)));
  }

  @Test
  public void shouldSupportDays() {
    assertThat(DurationParser.parse("98 Day"), is(Duration.ofDays(98)));
  }

  @Test
  public void shouldBuildDuration() {
    assertThat(DurationParser.buildDuration(20, "DAYS"), is(Duration.ofDays(20)));
  }
}