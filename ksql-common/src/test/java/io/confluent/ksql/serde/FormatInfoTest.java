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
import static io.confluent.ksql.serde.Format.DELIMITED;
import static io.confluent.ksql.serde.Format.KAFKA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FormatInfoTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .testAllPublicStaticMethods(FormatInfo.class);
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            FormatInfo.of(Format.DELIMITED, Optional.empty(), Optional.of(Delimiter.of('x'))),
            FormatInfo.of(Format.DELIMITED, Optional.empty(), Optional.of(Delimiter.of('x')))
        )
        .addEqualityGroup(
            FormatInfo.of(Format.AVRO, Optional.of("something"), Optional.empty()),
            FormatInfo.of(Format.AVRO, Optional.of("something"), Optional.empty())
        )
        .addEqualityGroup(
            FormatInfo.of(Format.AVRO, Optional.empty(), Optional.empty()),
            FormatInfo.of(Format.AVRO)
        )
        .addEqualityGroup(
            FormatInfo.of(Format.JSON, Optional.empty(), Optional.empty()),
            FormatInfo.of(Format.JSON)
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToStringAvro() {
    // Given:
    final FormatInfo info = FormatInfo.of(AVRO, Optional.of("something"), Optional.empty());

    // When:
    final String result = info.toString();

    // Then:
    assertThat(result, containsString("AVRO"));
    assertThat(result, containsString("something"));
  }

  @Test
  public void shouldImplementToStringDelimited() {
    // Given:
    final FormatInfo info = FormatInfo.of(DELIMITED, Optional.empty(), Optional.of(Delimiter.of("~")));

    // When:
    final String result = info.toString();

    // Then:
    assertThat(result, containsString("DELIMITED"));
    assertThat(result, containsString("~"));
  }

  @Test
  public void shouldThrowOnNonAvroWithAvroSchemName() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Full schema name only supported with AVRO format");

    // When:
    FormatInfo.of(Format.JSON, Optional.of("thing"), Optional.empty());
  }

  @Test
  public void shouldThrowOnEmptyAvroSchemaName() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Schema name cannot be empty");

    // When:
    FormatInfo.of(Format.AVRO, Optional.of(""), Optional.empty());
  }

  @Test
  public void shouldGetFormat() {
    assertThat(FormatInfo.of(KAFKA, Optional.empty(), Optional.empty()).getFormat(), is(KAFKA));
  }

  @Test
  public void shouldGetAvroSchemaName() {
    assertThat(FormatInfo.of(AVRO, Optional.of("Something"), Optional.empty()).getAvroFullSchemaName(),
        is(Optional.of("Something")));

    assertThat(FormatInfo.of(AVRO, Optional.empty(), Optional.empty()).getAvroFullSchemaName(),
        is(Optional.empty()));
  }

  @Test
  public void shouldThrowWhenAttemptingToUseValueDelimeterWithAvroFormat() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Delimeter only supported with DELIMITED format");

    // When:
    FormatInfo.of(Format.AVRO, Optional.of("something"), Optional.of(Delimiter.of('x')));
  }

  @Test
  public void shouldThrowWhenAttemptingToUseValueDelimeterWithJsonFormat() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Delimeter only supported with DELIMITED format");

    // When:
    FormatInfo.of(Format.JSON, Optional.empty(), Optional.of(Delimiter.of('x')));
  }
}