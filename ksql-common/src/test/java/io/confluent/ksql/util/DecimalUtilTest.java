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

package io.confluent.ksql.util;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class DecimalUtilTest {
  /* Set Decimal Schema under test */
  private static final int TEST_PRECISION = 6;
  private static final int TEST_SCALE = 2;
  private static final Schema TEST_DECIMAL_SCHEMA = DecimalUtil.schema(TEST_PRECISION, TEST_SCALE);

  @Test
  public void shouldReturnOptionalDecimalSchema() {
    assertThat(TEST_DECIMAL_SCHEMA.isOptional(), is(true));
  }

  @Test
  public void shouldReturnTrueOnConnectDecimalSchema() {
    assertThat(DecimalUtil.isDecimalSchema(TEST_DECIMAL_SCHEMA), is(true));
  }

  @Test
  public void shouldReturnPrecisionParameters() {
    assertThat(DecimalUtil.getPrecision(TEST_DECIMAL_SCHEMA), is(TEST_PRECISION));
  }

  @Test
  public void shouldReturnScaleParameters() {
    assertThat(DecimalUtil.getScale(TEST_DECIMAL_SCHEMA), is(TEST_SCALE));
  }

  @Test
  public void shouldReturnFalseOnNullSchema() {
    assertThat(DecimalUtil.isDecimalSchema(null), is(false));
  }

  @Test
  public void shouldReturnFalseOnEmptySchemaName() {
    assertThat(DecimalUtil.isDecimalSchema(SchemaBuilder.bytes().build()), is(false));
  }

  @Test
  public void shouldReturnFalseOnUnknownDecimalSchema() {
    assertThat(DecimalUtil.isDecimalSchema(SchemaBuilder.bytes().name("NoConnectDecimal")), is(false));
  }

  @Test(expected = NumberFormatException.class)
  public void shouldThrowNumberFormatExceptionIfPrecisionParameterIsNotNumeric() {
    DecimalUtil.getPrecision(SchemaBuilder.bytes().name(Decimal.LOGICAL_NAME)
        .parameter(DecimalUtil.PRECISION_FIELD, "1d").build());
  }

  @Test(expected = NumberFormatException.class)
  public void shouldThrowNumberFormatExceptionIfScaleParameterIsNotNumeric() {
    DecimalUtil.getScale(SchemaBuilder.bytes().name(Decimal.LOGICAL_NAME)
        .parameter(DecimalUtil.SCALE_FIELD, "1d").build());
  }
}
