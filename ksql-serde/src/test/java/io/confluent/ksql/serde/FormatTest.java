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

package io.confluent.ksql.serde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FormatTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldCreateFromString() {
    assertThat(Format.of(FormatInfo.of("JsoN")), is(Format.JSON));
    assertThat(Format.of(FormatInfo.of("AvRo")), is(Format.AVRO));
    assertThat(Format.of(FormatInfo.of("Delimited")), is(Format.DELIMITED));
  }

  @Test
  public void shouldHaveCorrectToString() {
    assertThat(Format.JSON.toString(), is("JSON"));
    assertThat(Format.AVRO.toString(), is("AVRO"));
    assertThat(Format.DELIMITED.toString(), is("DELIMITED"));
  }

  @Test
  public void shouldThrowOnUnknownFormat() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unknown format: bob");

    // When:
    Format.of(FormatInfo.of("bob"));
  }

  @Test
  public void shouldThrowOnNonAvroWithAvroSchemaName() {
    // Given:
    final FormatInfo format = FormatInfo.of("JSON", ImmutableMap.of(FormatInfo.FULL_SCHEMA_NAME, "foo"));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("JSON does not support the following configs: [fullSchemaName]");

    // When:
    Format.of(format);
  }

  @Test
  public void shouldThrowOnEmptyAvroSchemaName() {
    // Given:
    final FormatInfo format = FormatInfo.of("AVRO", ImmutableMap.of(FormatInfo.FULL_SCHEMA_NAME, " "));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("fullSchemaName cannot be empty. Format configuration: {fullSchemaName= }");

    // When:
    Format.of(format);
  }

  @Test
  public void shouldThrowWhenAttemptingToUseValueDelimiterWithJsonFormat() {
    // Given:
    final FormatInfo format = FormatInfo.of("JSON", ImmutableMap.of("delimiter", "x"));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("JSON does not support the following configs: [delimiter]");

    // When:
    Format.of(format);

  }

}