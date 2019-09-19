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
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("boolean"), SqlTypes.BOOLEAN)
        .build();
    
    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, schema);
      fail("Expecting exception: KsqlException");
    } catch (final KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceBooleanReturningBooleanWhereBooleanValueIsFalse() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("boolean"), SqlTypes.BOOLEAN)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(Boolean.FALSE, genericRowValueTypeEnforcer.enforceColumnType(0, "0x"));
  }

  @Test
  public void testEnforceBooleanReturningBooleanWhereBooleanValueIsTrue() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("boolean"), SqlTypes.BOOLEAN)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(Boolean.TRUE, genericRowValueTypeEnforcer.enforceColumnType(0, true));
  }

  @Test
  public void testEnforceBooleanReturningNull() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("boolean"), SqlTypes.BOOLEAN)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertNull(genericRowValueTypeEnforcer.enforceColumnType(0, null));
  }

  @Test
  public void testEnforceString() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("string"), SqlTypes.STRING)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, 0.0);
      fail("Expecting exception: KsqlException");
    } catch (final KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceStringReturningNull() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("string"), SqlTypes.STRING)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertNull(genericRowValueTypeEnforcer.enforceColumnType(0, null));
  }

  @Test
  public void testEnforceInteger() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, schema);
      fail("Expecting exception: KsqlException");
    } catch (final KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerThrowsNumberFormatExceptionOnInvalidCharSequence() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("Not A number"));
      fail("Expecting exception: NumberFormatException");
    } catch (final NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerThrowsNumberFormatExceptionOnInvalidString() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, "Wzhq'Rrv?s=O");
      fail("Expecting exception: NumberFormatException");
    } catch (final NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceIntegerOnValidCharSequence() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(55, genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("55")));
  }

  @Test
  public void testEnforceIntegerOnValidString() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-55, genericRowValueTypeEnforcer.enforceColumnType(0, "-55"));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerOne() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(55, genericRowValueTypeEnforcer.enforceColumnType(0, 55));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerThree() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(361, genericRowValueTypeEnforcer.enforceColumnType(0, 361L));
  }

  @Test
  public void testEnforceIntegerAndEnforceIntegerFour() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(1, genericRowValueTypeEnforcer.enforceColumnType(0, 1));
  }

  @Test
  public void testEnforceIntegerReturningNull() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("int"), SqlTypes.INTEGER)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertNull(genericRowValueTypeEnforcer.enforceColumnType(0, null));
  }

  @Test
  public void testEnforceLong() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, Boolean.FALSE);
      fail("Expecting exception: KsqlException");
    } catch (final KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongThrowsNumberFormatExceptionOnInvalidCharSequence() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("Not a Long"));
      fail("Expecting exception: NumberFormatException");
    } catch (final NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongThrowsNumberFormatExceptionOnInvalidString() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, "-drbetk");
      fail("Expecting exception: NumberFormatException");
    } catch (final NumberFormatException e) {
      assertEquals(NumberFormatException.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testEnforceLongOnValidCharSequence() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(123L, genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("123")));
  }

  @Test
  public void testEnforceLongOnValidString() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-123L, genericRowValueTypeEnforcer.enforceColumnType(0, "-123"));
  }

  @Test
  public void testEnforceLongAndEnforceLongOne() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(0L, genericRowValueTypeEnforcer.enforceColumnType(0, 0));
  }

  @Test
  public void testEnforceLongReturningLongWhereByteValueIsNegative() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-2315L, genericRowValueTypeEnforcer.enforceColumnType(0, -2315));
  }

  @Test
  public void testEnforceLongReturningLongWhereShortValueIsNegative() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-446L, genericRowValueTypeEnforcer.enforceColumnType(0, -446.28F));
  }

  @Test
  public void testEnforceLongReturningNull() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("long"), SqlTypes.BIGINT)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertNull(genericRowValueTypeEnforcer.enforceColumnType(0, null));
  }

  @Test
  public void testEnforceDouble() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);
    final Object object = new Object();

    try {
      genericRowValueTypeEnforcer.enforceColumnType(0, object);
      fail("Expecting exception: KsqlException");
    } catch (final KsqlException e) {
      assertEquals(GenericRowValueTypeEnforcer.class.getName(),
                   e.getStackTrace()[0].getClassName());
    }
  }

  @Test(expected = NumberFormatException.class)
  public void testEnforceDoubleThrowsNumberFormatExceptionOnInvalidCharSequence() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("not a double"));
  }

  @Test(expected = NumberFormatException.class)
  public void testEnforceDoubleThrowsNumberFormatExceptionOnInvalidString() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    genericRowValueTypeEnforcer.enforceColumnType(0, "not a double");
  }

  @Test
  public void testEnforceDoubleOnValidCharSequence() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(1.0, genericRowValueTypeEnforcer.enforceColumnType(0, new StringBuilder("1.0")));
  }

  @Test
  public void testEnforceDoubleOnValidString() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-1.0, genericRowValueTypeEnforcer.enforceColumnType(0, "-1.0"));
  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleOne() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(0.0, genericRowValueTypeEnforcer.enforceColumnType(0, 0));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereByteValueIsNegative() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals((-1.0), genericRowValueTypeEnforcer.enforceColumnType(0, -1));
  }

  @Test
  public void testEnforceDoubleAndEnforceDoubleTwo() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(0.0, genericRowValueTypeEnforcer.enforceColumnType(0, 0.0F));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsPositive() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(366.0, genericRowValueTypeEnforcer.enforceColumnType(0, 366L));
  }

  @Test
  public void testEnforceDoubleReturningDoubleWhereShortValueIsNegative() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertEquals(-433.0, genericRowValueTypeEnforcer.enforceColumnType(0, -433));
  }

  @Test
  public void testEnforceDoubleReturningNull() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("double"), SqlTypes.DOUBLE)
        .build();

    final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer =
        new GenericRowValueTypeEnforcer(schema);

    assertNull(genericRowValueTypeEnforcer.enforceColumnType(0, null));
  }
}
