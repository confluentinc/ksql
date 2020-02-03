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

import static io.confluent.ksql.serde.Format.AVRO;
import static io.confluent.ksql.serde.Format.KAFKA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.util.KsqlException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings("UnstableApiUsage")
public class FormatInfoTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            FormatInfo.of(Format.DELIMITED, ImmutableMap.of(FormatInfo.DELIMITER, "x")),
            FormatInfo.of(Format.DELIMITED, ImmutableMap.of(FormatInfo.DELIMITER, "x"))
        )
        .addEqualityGroup(
            FormatInfo.of(Format.AVRO, ImmutableMap.of(FormatInfo.FULL_SCHEMA_NAME, "something")),
            FormatInfo.of(Format.AVRO, ImmutableMap.of(FormatInfo.FULL_SCHEMA_NAME, "something"))
        )
        .addEqualityGroup(
            FormatInfo.of(Format.AVRO),
            FormatInfo.of(Format.AVRO)
        )
        .addEqualityGroup(
            FormatInfo.of(Format.JSON),
            FormatInfo.of(Format.JSON)
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToStringAvro() {
    // Given:
    final FormatInfo info = FormatInfo.of(AVRO, ImmutableMap.of(FormatInfo.FULL_SCHEMA_NAME, "something"));

    // When:
    final String result = info.toString();

    // Then:
    assertThat(result, containsString("AVRO"));
    assertThat(result, containsString("something"));
  }

  @Test
  public void shouldThrowOnNonAvroWithAvroSchemaName() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("JSON does not support the following configs: [fullSchemaName]");

    // When:
    FormatInfo.of(Format.JSON, ImmutableMap.of(FormatInfo.FULL_SCHEMA_NAME, "foo"));
  }

  @Test
  public void shouldThrowOnEmptyAvroSchemaName() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("fullSchemaName cannot be empty. Format configuration: {fullSchemaName= }");

    // When:
    FormatInfo.of(Format.AVRO, ImmutableMap.of(FormatInfo.FULL_SCHEMA_NAME, " "));
  }

  @Test
  public void shouldGetFormat() {
    assertThat(FormatInfo.of(KAFKA).getFormat(), is(KAFKA));
  }

  @Test
  public void shouldGetAvroSchemaName() {
    assertThat(FormatInfo.of(AVRO, ImmutableMap.of(FormatInfo.FULL_SCHEMA_NAME, "Something")).getProperties(),
        is(ImmutableMap.of(FormatInfo.FULL_SCHEMA_NAME, "Something")));

    assertThat(FormatInfo.of(AVRO).getProperties(), is(ImmutableMap.of()));
  }

  @Test
  public void shouldThrowWhenAttemptingToUseValueDelimiterWithJsonFormat() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("JSON does not support the following configs: [delimiter]");

    // When:
    FormatInfo.of(Format.JSON, ImmutableMap.of("delimiter", "x"));
  }
}