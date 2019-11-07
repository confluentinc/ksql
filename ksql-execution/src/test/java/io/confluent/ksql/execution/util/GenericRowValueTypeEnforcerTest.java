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

package io.confluent.ksql.execution.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

/**
 * Unit tests for class {@link GenericRowValueTypeEnforcer}.
 *
 * @see GenericRowValueTypeEnforcer
 **/
public class GenericRowValueTypeEnforcerTest {

  @Test
  public void testEnforceBoolean() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("boolean"), SqlTypes.BOOLEAN)
        .build();
    
    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, schema);
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceBooleanReturningBooleanWhereBooleanValueIsFalse() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("boolean"), SqlTypes.BOOLEAN)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(Boolean.FALSE, genericRowValueTypeEnforcer.enforceColumnType(0, "0x"));
  }

  @Test
  public void testEnforceBooleanReturningBooleanWhereBooleanValueIsTrue() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("boolean"), SqlTypes.BOOLEAN)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(Boolean.TRUE, genericRowValueTypeEnforcer.enforceColumnType(0, true));
  }

  @Test
  public void testEnforceBooleanReturningNull() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("boolean"), SqlTypes.BOOLEAN)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertNull(genericRowValueTypeEnforcer.enforceColumnType(0, null));
  }

  @Test
  public void testEnforceString() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("string"), SqlTypes.STRING)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, 0.0);
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceStringReturningNull() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("string"), SqlTypes.STRING)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertNull(genericRowValueTypeEnforcer.enforceColumnType(0, null));
  }

  @Test
  public void testEnforceInteger() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, schema);
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerThrowsNumberFormatExceptionOnInvalidCharSequence() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("Not A number"));
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerThrowsNumberFormatExceptionOnInvalidString() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, "Wzhq'Rrv?s=O");
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerOnValidCharSequence() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(55, genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("55")));
  }

  @Test
  public void testEnforceIntegerOnValidString() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-55, genericRowValueTypeEnforcer.enforceColumnType(0, "-55"));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerOne() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(55, genericRowValueTypeEnforcer.enforceColumnType(0, 55));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerThree() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(361, genericRowValueTypeEnforcer.enforceColumnType(0, 361L));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerFour() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(1, genericRowValueTypeEnforcer.enforceColumnType(0, 1));
  }

  @Test
  public void testEnforceIntegerReturningNull() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertNull(genericRowValueTypeEnforcer.enforceColumnType(0, null));
  }

  @Test
  public void testEnforceLong() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, Boolean.FALSE);
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongThrowsNumberFormatExceptionOnInvalidCharSequence() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("Not a Long"));
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongThrowsNumberFormatExceptionOnInvalidString() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, "-drbetk");
      fail("Expecting exception: NumberFormatException");
    } catch (NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongOnValidCharSequence() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(123L, genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("123")));
  }

  @Test
  public void testEnforceLongOnValidString() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-123L, genericRowValueTypeEnforcer.enforceColumnType(0, "-123"));
  }

  @Test
  public void testEnforceLongAndEnforceLongOne() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(0L, genericRowValueTypeEnforcer.enforceColumnType(0, 0));
  }

  @Test
  public void testEnforceLongReturningLongWhereByteValueIsNegative() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-2315L, genericRowValueTypeEnforcer.enforceColumnType(0, -2315));
  }

  @Test
  public void testEnforceLongReturningLongWhereShortValueIsNegative() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-446L, genericRowValueTypeEnforcer.enforceColumnType(0, -446.28F));
  }

  @Test
  public void testEnforceLongReturningNull() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertNull(genericRowValueTypeEnforcer.enforceColumnType(0, null));
  }

  @Test
  public void testEnforceDouble() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);
    Object object = new Object();

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, object);
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test(expected = NumberFormatException.class)
  public void testEnforceDoubleThrowsNumberFormatExceptionOnInvalidCharSequence() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("not a double"));
  }

  @Test(expected = NumberFormatException.class)
  public void testEnforceDoubleThrowsNumberFormatExceptionOnInvalidString() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    genericRowValueTypeEnforcer.enforceColumnType(0, "not a double");
  }

  @Test
  public void testEnforceDoubleOnValidCharSequence() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(1.0, genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("1.0")));
  }

  @Test
  public void testEnforceDoubleOnValidString() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-1.0, genericRowValueTypeEnforcer.enforceColumnType(0, "-1.0"));
  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleOne() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(0.0, genericRowValueTypeEnforcer.enforceColumnType(0, 0));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereByteValueIsNegative() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals((-1.0), genericRowValueTypeEnforcer.enforceColumnType(0, -1));
  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleTwo() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(0.0, genericRowValueTypeEnforcer.enforceColumnType(0, 0.0F));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsPositive() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(366.0, genericRowValueTypeEnforcer.enforceColumnType(0, 366L));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsNegative() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-433.0, genericRowValueTypeEnforcer.enforceColumnType(0, -433));
  }

  @Test
  public void testEnforceDoubleReturningNull() {
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertNull(genericRowValueTypeEnforcer.enforceColumnType(0, null));
  }
}
