/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.math.BigDecimal;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DecimalUtilTest {

  private static final Schema DECIMAL_SCHEMA = DecimalUtil.builder(2, 1).build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldBuildCorrectSchema() {
    // Then:
    assertThat(DECIMAL_SCHEMA, is(Decimal.builder(1).parameter("connect.decimal.precision", "2").optional().build()));
  }

  @Test
  public void shouldCopyBuilder() {
    // When:
    final Schema copy = DecimalUtil.builder(DECIMAL_SCHEMA).build();

    // Then:
    assertThat(copy, is(DECIMAL_SCHEMA));
  }

  @Test
  public void shouldCheckWhetherSchemaIsDecimal() {
    // Then:
    assertThat("Expected DECIMAL_SCHEMA to be isDecimal", DecimalUtil.isDecimal(DECIMAL_SCHEMA));
  }

  @Test
  public void shouldNotCheckSchemaForNonDecimals() {
    // Given:
    final Schema notDecimal = Schema.OPTIONAL_STRING_SCHEMA;

    // Then:
    assertThat("String should not be decimal schema", !DecimalUtil.isDecimal(notDecimal));
  }

  @Test
  public void shouldExtractScaleFromDecimalSchema() {
    // When:
    final int scale = DecimalUtil.scale(DECIMAL_SCHEMA);

    // Then:
    assertThat(scale, is(1));
  }

  @Test
  public void shouldExtractPrecisionFromDecimalSchema() {
    // When:
    final int scale = DecimalUtil.precision(DECIMAL_SCHEMA);

    // Then:
    assertThat(scale, is(2));
  }

  @Test
  public void shouldEnsureFitIfExactMatch() {
    // No Exception When:
    DecimalUtil.ensureFit(new BigDecimal("1.2"), DECIMAL_SCHEMA);
  }

  @Test
  public void shouldFailIfBuilderWithZeroPrecision() {
    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("DECIMAL precision must be >= 1");

    // When:
    DecimalUtil.builder(0, 0);
  }

  @Test
  public void shouldFailIfBuilderWithNegativeScale() {
    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("DECIMAL scale must be >= 0");

    // When:
    DecimalUtil.builder(1, -1);
  }

  @Test
  public void shouldFailIfBuilderWithScaleGTPrecision() {
    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("DECIMAL precision must be >= scale");

    // When:
    DecimalUtil.builder(1, 2);
  }

  @Test
  public void shouldFailFitIfNotExactMatchMoreDigits() {
    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Cannot fit decimal '12' into DECIMAL(2, 1)");

    // When:
    DecimalUtil.ensureFit(new BigDecimal("12"), DECIMAL_SCHEMA);
  }

  @Test
  public void shouldFailFitIfTruncationNecessary() {
    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Cannot fit decimal '1.23' into DECIMAL(2, 1) without rounding.");

    // When:
    DecimalUtil.ensureFit(new BigDecimal("1.23"), DECIMAL_SCHEMA);
  }
}