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

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class DecimalUtilTest {
  @Test
  public void shouldReturnOptionalDecimalSchema() {
    final int anyValue = 1;

    assertThat(DecimalUtil.schema(anyValue, anyValue).isOptional(), is(true));
  }

  @Test
  public void shouldReturnTrueOnConnectDecimalSchema() {
    final int anyValue = 1;

    assertThat(DecimalUtil.isDecimalSchema(DecimalUtil.schema(anyValue, anyValue)), is(true));
  }

  @Test
  public void shouldReturnFalseOnUnknownConnectDecimalSchema() {
    assertThat(DecimalUtil.isDecimalSchema(null), is(false));
    assertThat(DecimalUtil.isDecimalSchema(SchemaBuilder.bytes().build()), is(false));
    assertThat(DecimalUtil.isDecimalSchema(SchemaBuilder.bytes().name("NoConnectDecimal")), is(false));
  }

  @Test
  public void shouldReturnPrecisionAndScaleSchemaParameters() {
    final int precision = 6;
    final int scale = 2;

    assertThat(DecimalUtil.getPrecision(DecimalUtil.schema(precision, scale)), is(precision));
    assertThat(DecimalUtil.getScale(DecimalUtil.schema(precision, scale)), is(scale));
  }
}
