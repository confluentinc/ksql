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

package io.confluent.ksql.serde.json;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import io.confluent.ksql.schema.ksql.PersistenceSchema;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JsonSerdeUtilsTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldConvertToBooleanCorrectly() {
    final Boolean b = JsonSerdeUtils.toBoolean(true);
    assertThat(b, equalTo(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonBooleanToBoolean() {
    JsonSerdeUtils.toBoolean(1);
  }

  @Test
  public void shouldConvertToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger(1);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertLongToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger(1L);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertDoubleToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger(1.0);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertStringToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger("1");
    assertThat(i, equalTo(1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotConvertIncorrectStringToInt() {
    JsonSerdeUtils.toInteger("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonIntegerToIntegr() {
    JsonSerdeUtils.toInteger(true);
  }

  @Test
  public void shouldConvertToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong(1L);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertIntToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong(1);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertDoubleToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong(1.0);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertStringToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong("1");
    assertThat(l, equalTo(1L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotConvertIncorrectStringToLong() {
    JsonSerdeUtils.toLong("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleLong() {
    JsonSerdeUtils.toInteger(true);
  }

  @Test
  public void shouldConvertToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble(1.0);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertIntToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble(1);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertLongToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble(1L);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertStringToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble("1.0");
    assertThat(d, equalTo(1.0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotConvertIncorrectStringToDouble() {
    JsonSerdeUtils.toDouble("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleDouble() {
    JsonSerdeUtils.toDouble(true);
  }

  @Test
  public void shouldNotIncludeValueInExceptionWhenFailingToBoolean() {
    try {
      // When:
      JsonSerdeUtils.toBoolean("personal info: do not log me");

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  @Test
  public void shouldNotIncludeValueInExceptionWhenFailingToInteger() {
    try {
      // When:
      JsonSerdeUtils.toInteger("personal info: do not log me");

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  @Test
  public void shouldNotIncludeValueInExceptionWhenFailingToLong() {
    try {
      // When:
      JsonSerdeUtils.toLong("personal info: do not log me");

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  @Test
  public void shouldNotIncludeValueInExceptionWhenFailingToDouble() {
    try {
      // When:
      JsonSerdeUtils.toDouble("personal info: do not log me");

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  @Test
  public void shouldThrowOnMapWithNoneStringKeys() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Only MAPs with STRING keys are supported");

    //  When:
    JsonSerdeUtils.validateSchema(persistenceSchema(
        SchemaBuilder
            .map(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
            .build()
    ));
  }

  @Test
  public void shouldThrowOnNestedMapWithNoneStringKeys() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Only MAPs with STRING keys are supported");

    //  When:
    JsonSerdeUtils.validateSchema(persistenceSchema(
        SchemaBuilder
            .struct()
            .field("f0", SchemaBuilder
                .map(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build())
            .build()
    ));
  }

  private static PersistenceSchema persistenceSchema(final Schema schema) {
    return PersistenceSchema.from(
        (ConnectSchema) SchemaBuilder.struct()
            .field("field", schema)
            .build(),
        true
    );
  }
}
