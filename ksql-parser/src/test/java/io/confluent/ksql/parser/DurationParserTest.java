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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class DurationParserTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldThrowOnTooFewTokens() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected 2 tokens, got: 1");

    // When:
    DurationParser.parse("10");
  }

  @Test
  public void shouldThrowOnTooManyTokens() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected 2 tokens, got: 3");

    // When:
    DurationParser.parse("10 Seconds Long");
  }

  @Test
  public void shouldThrowOnNonNumericDuration() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Not numeric: '10s'");

    // When:
    DurationParser.parse("10s Seconds");
  }

  @Test
  public void shouldThrowOnUnknownTimeUnit() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unknown time unit: 'GREEN_BOTTLES'");

    // When:
    DurationParser.parse("10 Green_Bottles");
  }

  @Test
  public void shouldIncludeOriginalInExceptionMessage() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid duration: '10 Green_Bottles'. ");

    // When:
    DurationParser.parse("10 Green_Bottles");
  }

  @Test
  public void shouldIncludeValidTimeUnitsInExceptionMessage() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Supported time units are: "
        + "NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS");

    // When:
    DurationParser.parse("10 Bananas");
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
}