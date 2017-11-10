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

import org.junit.Test;
import static org.junit.Assert.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Unit tests for class {@link GenericRowValueTypeEnforcer}.
 *
 * @see GenericRowValueTypeEnforcer
 *
 **/
public class GenericRowValueTypeEnforcerTest{
  @Test
  public void testEnforceBoolean() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      try { 
        genericRowValueTypeEnforcer.enforceBoolean(schemaBuilder);
        fail("Expecting exception: KsqlException");
      } catch(KsqlException e) {
         assertEquals(GenericRowValueTypeEnforcer.class.getName(), e.getStackTrace()[0].getClassName());
      }
  }

  @Test
  public void testEnforceBooleanReturningBooleanWhereBooleanValueIsFalse() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertFalse(genericRowValueTypeEnforcer.enforceBoolean("0x"));
  }

  @Test
  public void testEnforceBooleanReturningBooleanWhereBooleanValueIsTrue() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertTrue(genericRowValueTypeEnforcer.enforceBoolean(true));
  }

  @Test
  public void testEnforceBooleanReturningNull() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertNull(genericRowValueTypeEnforcer.enforceBoolean( null));
  }

  @Test
  public void testEnforceString() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      try { 
        genericRowValueTypeEnforcer.enforceString(0.0);
        fail("Expecting exception: KsqlException");
      } catch(KsqlException e) {
         assertEquals(GenericRowValueTypeEnforcer.class.getName(), e.getStackTrace()[0].getClassName());
      }
  }

  @Test
  public void testEnforceStringReturningNull() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertNull(genericRowValueTypeEnforcer.enforceString( null));
  }

  @Test
  public void testEnforceInteger() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      try { 
        genericRowValueTypeEnforcer.enforceInteger(schemaBuilder);
        fail("Expecting exception: KsqlException");
      } catch(KsqlException e) {
         assertEquals(GenericRowValueTypeEnforcer.class.getName(), e.getStackTrace()[0].getClassName());
      }
  }

  @Test
  public void testEnforceIntegerThrowsNumberFormatExceptionOne() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);
      Charset charset = Charset.defaultCharset();
      ByteBuffer byteBuffer = charset.encode("com.google.common.primitives.Doubles$DoubleConverter");
      CharBuffer charBuffer = charset.decode(byteBuffer);

      try { 
        genericRowValueTypeEnforcer.enforceInteger(charBuffer);
        fail("Expecting exception: NumberFormatException");
      } catch(NumberFormatException e) {
         assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
      }
  }

  @Test
  public void testEnforceIntegerThrowsNumberFormatExceptionTwo() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      try { 
        genericRowValueTypeEnforcer.enforceInteger("Wzhq'Rrv?s=O");
        fail("Expecting exception: NumberFormatException");
      } catch(NumberFormatException e) {
         assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
      }
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerOne() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertEquals(55, (int) genericRowValueTypeEnforcer.enforceInteger(55));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerThree() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertEquals(361, (int) genericRowValueTypeEnforcer.enforceInteger(361L));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerFour() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertEquals(1, (int) genericRowValueTypeEnforcer.enforceInteger(1));
  }

  @Test
  public void testEnforceIntegerReturningNull() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertNull(genericRowValueTypeEnforcer.enforceInteger( null));
  }

  @Test
  public void testEnforceLong() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      try { 
        genericRowValueTypeEnforcer.enforceLong(Boolean.FALSE);
        fail("Expecting exception: KsqlException");
      } catch(KsqlException e) {
         assertEquals(GenericRowValueTypeEnforcer.class.getName(), e.getStackTrace()[0].getClassName());
      }
  }

  @Test
  public void testEnforceLongThrowsNumberFormatExceptionOne() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);
      Charset charset = Charset.defaultCharset();
      ByteBuffer byteBuffer = charset.encode("$f{D5-44`A");
      CharBuffer charBuffer = charset.decode(byteBuffer);

      try { 
        genericRowValueTypeEnforcer.enforceLong(charBuffer);
        fail("Expecting exception: NumberFormatException");
      } catch(NumberFormatException e) {
         assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
      }
  }

  @Test
  public void testEnforceLongThrowsNumberFormatExceptionTwo() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      try { 
        genericRowValueTypeEnforcer.enforceLong("-drbetk");
        fail("Expecting exception: NumberFormatException");
      } catch(NumberFormatException e) {
         assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
      }
  }

  @Test
  public void testEnforceLongAndEnforceLongOne() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertEquals(0L, (long) genericRowValueTypeEnforcer.enforceLong(0));
  }

  @Test
  public void testEnforceLongReturningLongWhereByteValueIsNegative() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertEquals(-2315L, (long) genericRowValueTypeEnforcer.enforceLong(-2315));
  }

  @Test
  public void testEnforceLongReturningLongWhereShortValueIsNegative() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertEquals(-446L, (long) genericRowValueTypeEnforcer.enforceLong(-446.28F));
  }

  @Test
  public void testEnforceLongReturningNull() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertNull(genericRowValueTypeEnforcer.enforceLong( null));
  }

  @Test
  public void testEnforceDouble() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);
      Object object = new Object();

      try { 
        genericRowValueTypeEnforcer.enforceDouble(object);
        fail("Expecting exception: KsqlException");
      } catch(KsqlException e) {
         assertEquals(GenericRowValueTypeEnforcer.class.getName(), e.getStackTrace()[0].getClassName());
      }
  }

  @Test(expected = NumberFormatException.class)
  public void testEnforceDoubleThrowsNumberFormatExceptionOne() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);
      Charset charset = Charset.defaultCharset();
      ByteBuffer byteBuffer = charset.encode("");
      CharBuffer charBuffer = charset.decode(byteBuffer);

      genericRowValueTypeEnforcer.enforceDouble(charBuffer);

  }

  @Test(expected = NumberFormatException.class)
  public void testEnforceDoubleThrowsNumberFormatExceptionTwo() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      genericRowValueTypeEnforcer.enforceDouble("0x");

  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleOne() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertEquals(0.0, genericRowValueTypeEnforcer.enforceDouble(0), 0.01);
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereByteValueIsNegative() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertEquals((-1.0), genericRowValueTypeEnforcer.enforceDouble(-1), 0.01);
  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleTwo() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertEquals(0.0,  genericRowValueTypeEnforcer.enforceDouble(0.0F), 0.01);
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsPositive() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertEquals(366.0, genericRowValueTypeEnforcer.enforceDouble(366L), 0.01);
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsNegative() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertEquals(-433.0, genericRowValueTypeEnforcer.enforceDouble(-433), 0.01);
  }

  @Test
  public void testEnforceDoubleReturningNull() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);

      assertNull(genericRowValueTypeEnforcer.enforceDouble( null));
  }

  @Test
  public void testEnforceFieldTypeTaking1And1WithNull() {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schemaBuilder);
      Schema.Type schema_Type = Schema.Type.BOOLEAN;
      ConnectSchema connectSchema = new ConnectSchema(schema_Type);

      try { 
        genericRowValueTypeEnforcer.enforceFieldType(connectSchema,  null);
        fail("Expecting exception: KsqlException");
      } catch(KsqlException e) {
         assertEquals(GenericRowValueTypeEnforcer.class.getName(), e.getStackTrace()[0].getClassName());
      }
  }
}