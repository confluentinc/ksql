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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

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
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
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
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(Boolean.FALSE, genericRowValueTypeEnforcer.enforceFieldType(0, "0x"));
  }

  @Test
  public void testEnforceBooleanReturningBooleanWhereBooleanValueIsTrue() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("boolean", SchemaBuilder.bool());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(Boolean.TRUE, genericRowValueTypeEnforcer.enforceFieldType(0, true));
  }

  @Test
  public void testEnforceBooleanReturningNull() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("boolean", SchemaBuilder.bool());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceString() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("string", SchemaBuilder.string());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
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
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceInteger() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
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
  public void testEnforceIntegerThrowsNumberFormatExceptionOne() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);
    Charset charset = Charset.defaultCharset();
    ByteBuffer byteBuffer = charset.encode("com.google.common.primitives.Doubles$DoubleConverter");
    CharBuffer charBuffer = charset.decode(byteBuffer);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, charBuffer);
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerThrowsNumberFormatExceptionTwo() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, "Wzhq'Rrv?s=O");
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerOne() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(55, (int) genericRowValueTypeEnforcer.enforceFieldType(0, 55));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerThree() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(361, (int) genericRowValueTypeEnforcer.enforceFieldType(0, 361L));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerFour() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(1, (int) genericRowValueTypeEnforcer.enforceFieldType(0, 1));
  }

  @Test
  public void testEnforceIntegerReturningNull() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int", SchemaBuilder.int32());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceLong() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
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
  public void testEnforceLongThrowsNumberFormatExceptionOne() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);
    Charset charset = Charset.defaultCharset();
    ByteBuffer byteBuffer = charset.encode("$f{D5-44`A");
    CharBuffer charBuffer = charset.decode(byteBuffer);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, charBuffer);
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongThrowsNumberFormatExceptionTwo() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    try {
      genericRowValueTypeEnforcer.enforceFieldType(0, "-drbetk");
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongAndEnforceLongOne() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(0L, (long) genericRowValueTypeEnforcer.enforceFieldType(0, 0));
  }

  @Test
  public void testEnforceLongReturningLongWhereByteValueIsNegative() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(-2315L, (long) genericRowValueTypeEnforcer.enforceFieldType(0, -2315));
  }

  @Test
  public void testEnforceLongReturningLongWhereShortValueIsNegative() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(-446L, (long) genericRowValueTypeEnforcer.enforceFieldType(0, -446.28F));
  }

  @Test
  public void testEnforceLongReturningNull() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("long", SchemaBuilder.int64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }

  @Test
  public void testEnforceDouble() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
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
  public void testEnforceDoubleThrowsNumberFormatExceptionOne() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);
    Charset charset = Charset.defaultCharset();
    ByteBuffer byteBuffer = charset.encode("");
    CharBuffer charBuffer = charset.decode(byteBuffer);

    genericRowValueTypeEnforcer.enforceFieldType(0, charBuffer);

  }

  @Test(expected = NumberFormatException.class)
  public void testEnforceDoubleThrowsNumberFormatExceptionTwo() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    genericRowValueTypeEnforcer.enforceFieldType(0, "0x");

  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleOne() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(0.0, (Double) genericRowValueTypeEnforcer.enforceFieldType(0, 0), 0.01);
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereByteValueIsNegative() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals((-1.0), (Double) genericRowValueTypeEnforcer.enforceFieldType(0, -1), 0.01);
  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleTwo() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(0.0, (Double) genericRowValueTypeEnforcer.enforceFieldType(0, 0.0F), 0.01);
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsPositive() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(366.0, (Double) genericRowValueTypeEnforcer.enforceFieldType(0, 366L), 0.01);
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsNegative() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertEquals(-433.0, (Double) genericRowValueTypeEnforcer.enforceFieldType(0, -433), 0.01);
  }

  @Test
  public void testEnforceDoubleReturningNull() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("double", SchemaBuilder.float64());
    GenericRowValueTypeEnforcer
        genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schemaBuilder);

    assertNull(genericRowValueTypeEnforcer.enforceFieldType(0, null));
  }
}