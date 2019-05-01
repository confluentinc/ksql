/*
 * Copyright 2018 Confluent Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.confluent.ksql.schema.ksql.KsqlSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

/**
 * Unit tests for class {@link GenericRowValueTypeEnforcer}.
 *
 * @see GenericRowValueTypeEnforcer
 **/
public class GenericRowValueTypeEnforcerTest {

  @Test
  public void testEnforceBoolean() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, schemaBuilder);
      fail("Expecting exception: KsqlException");
    } catch (final KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceBooleanReturningBooleanWhereBooleanValueIsFalse() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(Boolean.FALSE, genericRowValueTypeEnforcer.enforceFieldType(0, "0x"));
  }

  @Test
  public void testEnforceBooleanReturningBooleanWhereBooleanValueIsTrue() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(Boolean.TRUE, genericRowValueTypeEnforcer.enforceFieldType(0, true));
  }

  @Test
  public void testEnforceBooleanReturningNull() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceString() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("string", Schema.OPTIONAL_STRING_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, 0.0);
      fail("Expecting exception: KsqlException");
    } catch (final KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceStringReturningNull() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("string", Schema.OPTIONAL_STRING_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceInteger() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("int", Schema.OPTIONAL_INT32_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, schemaBuilder);
      fail("Expecting exception: KsqlException");
    } catch (final KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerThrowsNumberFormatExceptionOnInvalidCharSequence() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("int", Schema.OPTIONAL_INT32_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("Not A number"));
      fail("Expecting exception: NumberFormatException");
    } catch (final NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerThrowsNumberFormatExceptionOnInvalidString() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("int", Schema.OPTIONAL_INT32_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, "Wzhq'Rrv?s=O");
      fail("Expecting exception: NumberFormatException");
    } catch (final NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerOnValidCharSequence() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("int", Schema.OPTIONAL_INT32_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(55, genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("55")));
  }

  @Test
  public void testEnforceIntegerOnValidString() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("int", Schema.OPTIONAL_INT32_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(-55, genericRowValueTypeEnforcer.enforceFieldType(0, "-55"));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerOne() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("int", Schema.OPTIONAL_INT32_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(55, genericRowValueTypeEnforcer.enforceFieldType(0, 55));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerThree() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("int", Schema.OPTIONAL_INT32_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(361, genericRowValueTypeEnforcer.enforceFieldType(0, 361L));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerFour() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("int", Schema.OPTIONAL_INT32_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(1, genericRowValueTypeEnforcer.enforceFieldType(0, 1));
  }

  @Test
  public void testEnforceIntegerReturningNull() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("int", Schema.OPTIONAL_INT32_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceLong() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("long", Schema.OPTIONAL_INT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, Boolean.FALSE);
      fail("Expecting exception: KsqlException");
    } catch (final KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongThrowsNumberFormatExceptionOnInvalidCharSequence() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("long", Schema.OPTIONAL_INT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("Not a Long"));
      fail("Expecting exception: NumberFormatException");
    } catch (final NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongThrowsNumberFormatExceptionOnInvalidString() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("long", Schema.OPTIONAL_INT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, "-drbetk");
      fail("Expecting exception: NumberFormatException");
    } catch (final NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongOnValidCharSequence() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("long", Schema.OPTIONAL_INT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(123L, genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("123")));
  }

  @Test
  public void testEnforceLongOnValidString() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("long", Schema.OPTIONAL_INT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(-123L, genericRowValueTypeEnforcer.enforceFieldType(0, "-123"));
  }

  @Test
  public void testEnforceLongAndEnforceLongOne() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("long", Schema.OPTIONAL_INT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(0L, genericRowValueTypeEnforcer.enforceFieldType(0, 0));
  }

  @Test
  public void testEnforceLongReturningLongWhereByteValueIsNegative() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("long", Schema.OPTIONAL_INT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(-2315L, genericRowValueTypeEnforcer.enforceFieldType(0, -2315));
  }

  @Test
  public void testEnforceLongReturningLongWhereShortValueIsNegative() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("long", Schema.OPTIONAL_INT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(-446L, genericRowValueTypeEnforcer.enforceFieldType(0, -446.28F));
  }

  @Test
  public void testEnforceLongReturningNull() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("long", Schema.OPTIONAL_INT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceDouble() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));
    final Object object = new Object();

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, object);
      fail("Expecting exception: KsqlException");
    } catch (final KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test(expected = NumberFormatException.class)
  public void testEnforceDoubleThrowsNumberFormatExceptionOnInvalidCharSequence() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("not a double"));
  }

  @Test(expected = NumberFormatException.class)
  public void testEnforceDoubleThrowsNumberFormatExceptionOnInvalidString() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    genericRowValueTypeEnforcer.enforceFieldType(0, "not a double");
  }

  @Test
  public void testEnforceDoubleOnValidCharSequence() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(1.0, genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("1.0")));
  }

  @Test
  public void testEnforceDoubleOnValidString() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(-1.0, genericRowValueTypeEnforcer.enforceFieldType(0, "-1.0"));
  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleOne() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(0.0, genericRowValueTypeEnforcer.enforceFieldType(0, 0));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereByteValueIsNegative() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals((-1.0), genericRowValueTypeEnforcer.enforceFieldType(0, -1));
  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleTwo() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(0.0, genericRowValueTypeEnforcer.enforceFieldType(0, 0.0F));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsPositive() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(366.0, genericRowValueTypeEnforcer.enforceFieldType(0, 366L));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsNegative() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertEquals(-433.0, genericRowValueTypeEnforcer.enforceFieldType(0, -433));
  }

  @Test
  public void testEnforceDoubleReturningNull() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(KsqlSchema.of(schemaBuilder.build()));

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }
}