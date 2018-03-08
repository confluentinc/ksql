/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Unit tests for class {@link GenericRowValueTypeEnforcer}.
 *
 * @see GenericRowValueTypeEnforcer
 **/
public class GenericRowValueTypeEnforcerTest {

  @Test
  public void testEnforceBoolean() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("boolean", SchemaBuilder.bool());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, schemaBuilder);
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceBooleanReturningBooleanWhereBooleanValueIsFalse() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("boolean", SchemaBuilder.bool());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(Boolean.FALSE, genericRowValueTypeEnforcer.enforceFieldType(0, "0x"));
  }

  @Test
  public void testEnforceBooleanReturningBooleanWhereBooleanValueIsTrue() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("boolean", SchemaBuilder.bool());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(Boolean.TRUE, genericRowValueTypeEnforcer.enforceFieldType(0, true));
  }

  @Test
  public void testEnforceBooleanReturningNull() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("boolean", SchemaBuilder.bool());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceString() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("string", SchemaBuilder.string());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, 0.0);
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceStringReturningNull() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("string", SchemaBuilder.string());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceInteger() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, schemaBuilder);
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerThrowsNumberFormatExceptionOnInvalidCharSequence() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("Not A number"));
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerThrowsNumberFormatExceptionOnInvalidString() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, "Wzhq'Rrv?s=O");
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerOnValidCharSequence() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(55, genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("55")));
  }

  @Test
  public void testEnforceIntegerOnValidString() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(-55, genericRowValueTypeEnforcer.enforceFieldType(0, "-55"));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerOne() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(55, genericRowValueTypeEnforcer.enforceFieldType(0, 55));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerThree() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(361, genericRowValueTypeEnforcer.enforceFieldType(0, 361L));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerFour() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(1, genericRowValueTypeEnforcer.enforceFieldType(0, 1));
  }

  @Test
  public void testEnforceIntegerReturningNull() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceLong() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, Boolean.FALSE);
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongThrowsNumberFormatExceptionOnInvalidCharSequence() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("Not a Long"));
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongThrowsNumberFormatExceptionOnInvalidString() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, "-drbetk");
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongOnValidCharSequence() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(123L, genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("123")));
  }

  @Test
  public void testEnforceLongOnValidString() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(-123L, genericRowValueTypeEnforcer.enforceFieldType(0, "-123"));
  }

  @Test
  public void testEnforceLongAndEnforceLongOne() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(0L, genericRowValueTypeEnforcer.enforceFieldType(0, 0));
  }

  @Test
  public void testEnforceLongReturningLongWhereByteValueIsNegative() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(-2315L, genericRowValueTypeEnforcer.enforceFieldType(0, -2315));
  }

  @Test
  public void testEnforceLongReturningLongWhereShortValueIsNegative() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(-446L, genericRowValueTypeEnforcer.enforceFieldType(0, -446.28F));
  }

  @Test
  public void testEnforceLongReturningNull() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceDouble() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);
    Object object = new Object();

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, object);
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test(expected = NumberFormatException.class)
  public void testEnforceDoubleThrowsNumberFormatExceptionOnInvalidCharSequence() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("not a double"));
  }

  @Test(expected = NumberFormatException.class)
  public void testEnforceDoubleThrowsNumberFormatExceptionOnInvalidString() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    genericRowValueTypeEnforcer.enforceFieldType(0, "not a double");
  }

  @Test
  public void testEnforceDoubleOnValidCharSequence() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(1.0, genericRowValueTypeEnforcer.enforceFieldType(0, new StringBuilder("1.0")));
  }

  @Test
  public void testEnforceDoubleOnValidString() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(-1.0, genericRowValueTypeEnforcer.enforceFieldType(0, "-1.0"));
  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleOne() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(0.0, genericRowValueTypeEnforcer.enforceFieldType(0, 0));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereByteValueIsNegative() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals((-1.0), genericRowValueTypeEnforcer.enforceFieldType(0, -1));
  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleTwo() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(0.0, genericRowValueTypeEnforcer.enforceFieldType(0, 0.0F));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsPositive() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(366.0, genericRowValueTypeEnforcer.enforceFieldType(0, 366L));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsNegative() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(-433.0, genericRowValueTypeEnforcer.enforceFieldType(0, -433));
  }

  @Test
  public void testEnforceDoubleReturningNull() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }
}