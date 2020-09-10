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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JsonSerdeUtilsTest {

  @Mock
  private ObjectMapper mapper;

  @Test
  public void shouldConvertToBooleanCorrectly() {
    final Boolean b = JsonSerdeUtils.toBoolean(BooleanNode.TRUE);
    assertThat(b, equalTo(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonBooleanToBoolean() {
    JsonSerdeUtils.toBoolean(JsonNodeFactory.instance.numberNode(1));
  }

  @Test
  public void shouldConvertToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger(JsonNodeFactory.instance.numberNode(1));
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertLongToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger(JsonNodeFactory.instance.numberNode(1L));
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertDoubleToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger(JsonNodeFactory.instance.numberNode(1.0d));
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertStringToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger(JsonNodeFactory.instance.textNode("1"));
    assertThat(i, equalTo(1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotConvertIncorrectStringToInt() {
    JsonSerdeUtils.toInteger(JsonNodeFactory.instance.textNode("1!"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonIntegerToIntegr() {
    JsonSerdeUtils.toInteger(JsonNodeFactory.instance.booleanNode(true));
  }

  @Test
  public void shouldConvertToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong(JsonNodeFactory.instance.numberNode(1L));
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertIntToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong(JsonNodeFactory.instance.numberNode(1));
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertDoubleToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong(JsonNodeFactory.instance.numberNode(1.0d));
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertStringToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong(JsonNodeFactory.instance.textNode("1"));
    assertThat(l, equalTo(1L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotConvertIncorrectStringToLong() {
    JsonSerdeUtils.toLong(JsonNodeFactory.instance.textNode("1!"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleLong() {
    JsonSerdeUtils.toInteger(JsonNodeFactory.instance.booleanNode(true));
  }

  @Test
  public void shouldConvertToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble(JsonNodeFactory.instance.numberNode(1.0d));
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertIntToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble(JsonNodeFactory.instance.numberNode(1));
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertLongToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble(JsonNodeFactory.instance.numberNode(1L));
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertStringToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble(JsonNodeFactory.instance.textNode("1.0"));
    assertThat(d, equalTo(1.0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotConvertIncorrectStringToDouble() {
    JsonSerdeUtils.toDouble(JsonNodeFactory.instance.textNode("1!::"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleDouble() {
    JsonSerdeUtils.toDouble(JsonNodeFactory.instance.booleanNode(true));
  }

  @Test
  public void shouldNotIncludeValueInExceptionWhenFailingToBoolean() {
    try {
      // When:
      JsonSerdeUtils.toBoolean(JsonNodeFactory.instance.textNode("personal info: do not log me"));

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  @Test
  public void shouldNotIncludeValueInExceptionWhenFailingToInteger() {
    try {
      // When:
      JsonSerdeUtils.toInteger(JsonNodeFactory.instance.textNode("personal info: do not log me"));

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  @Test
  public void shouldNotIncludeValueInExceptionWhenFailingToLong() {
    try {
      // When:
      JsonSerdeUtils.toLong(JsonNodeFactory.instance.textNode("personal info: do not log me"));

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  @Test
  public void shouldNotIncludeValueInExceptionWhenFailingToDouble() {
    try {
      // When:
      JsonSerdeUtils.toDouble(JsonNodeFactory.instance.textNode("personal info: do not log me"));

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  @Test
  public void shouldThrowOnMapWithNoneStringKeys() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> JsonSerdeUtils.validateSchema(persistenceSchema(
            SchemaBuilder
                .map(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
                .build()
        ))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "JSON only supports MAP types with STRING keys"));
  }

  @Test
  public void shouldThrowOnNestedMapWithNoneStringKeys() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> JsonSerdeUtils.validateSchema(persistenceSchema(
            SchemaBuilder
                .struct()
                .field("f0", SchemaBuilder
                    .map(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build())
                .build()
        ))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "JSON only supports MAP types with STRING keys"));
  }

  @Test
  public void shouldSetCorrectOffsetWithMagicByte() throws IOException {
    // Given:
    byte[] json = new byte[]{/* magic */ 0x00, /* id */ 0x00, 0x00, 0x00, 0x01, /* data */ 0x01};

    // When:
    JsonSerdeUtils.readJsonSR(json, mapper, Object.class);

    // Then:
    Mockito.verify(mapper, Mockito.times(1)).readValue(json, 5, 1, Object.class);
  }

  @Test()
  public void shouldThrowOnStandardJsonConversion() throws IOException {
    // Given:
    byte[] json = new byte[]{/* data */ 0x01};

    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> JsonSerdeUtils.readJsonSR(json, mapper, Object.class)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Got unexpected JSON serialization format that did not start with the magic byte"));
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
