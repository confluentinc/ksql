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

import static io.confluent.ksql.serde.FormatFactory.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class FormatFactoryTest {

  @Test
  public void shouldCreateFromString() {
    assertThat(FormatFactory.of(FormatInfo.of("JsoN")), is(FormatFactory.JSON));
    assertThat(FormatFactory.of(FormatInfo.of("AvRo")), is(FormatFactory.AVRO));
    assertThat(FormatFactory.of(FormatInfo.of("Delimited")), is(FormatFactory.DELIMITED));
  }

  @Test
  public void shouldThrowOnUnknownFormat() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> of(FormatInfo.of("bob"))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown format: BOB"));
  }

  @Test
  public void shouldThrowOnNonAvroWithAvroSchemaName() {
    // Given:
    final FormatInfo format = FormatInfo.of("JSON", ImmutableMap.of(AvroFormat.FULL_SCHEMA_NAME, "foo"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> FormatFactory.of(format)
    );

    // Then:
    assertThat(e.getMessage(), containsString("JSON does not support the following configs: [fullSchemaName]"));
  }

  @Test
  public void shouldThrowOnEmptyAvroSchemaName() {
    // Given:
    final FormatInfo format = FormatInfo.of("AVRO", ImmutableMap.of(AvroFormat.FULL_SCHEMA_NAME, " "));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> FormatFactory.of(format)
    );

    // Then:
    assertThat(e.getMessage(), containsString("fullSchemaName cannot be empty. Format configuration: {fullSchemaName= }"));
  }

  @Test
  public void shouldThrowWhenAttemptingToUseValueDelimiterWithJsonFormat() {
    // Given:
    final FormatInfo format = FormatInfo.of("JSON", ImmutableMap.of("delimiter", "x"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> FormatFactory.of(format)
    );

    // Then:
    assertThat(e.getMessage(), containsString("JSON does not support the following configs: [delimiter]"));

  }

  @Test
  public void shouldCreateFromNameWithCaseInsensitivity() {
    // When:
    final Format format = FormatFactory.fromName("aVrO");

    // Then:
    assertThat(format, is(FormatFactory.AVRO));
  }

}