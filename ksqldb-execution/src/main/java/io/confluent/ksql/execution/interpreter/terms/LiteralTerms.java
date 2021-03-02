/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.execution.interpreter.terms.TypedTerms.BooleanTerm;
import io.confluent.ksql.execution.interpreter.terms.TypedTerms.DecimalTerm;
import io.confluent.ksql.execution.interpreter.terms.TypedTerms.DoubleTerm;
import io.confluent.ksql.execution.interpreter.terms.TypedTerms.IntegerTerm;
import io.confluent.ksql.execution.interpreter.terms.TypedTerms.LongTerm;
import io.confluent.ksql.execution.interpreter.terms.TypedTerms.StringTerm;
import io.confluent.ksql.execution.interpreter.terms.TypedTerms.TimestampTerm;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.math.BigDecimal;
import java.sql.Timestamp;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public final class LiteralTerms {

  private LiteralTerms() { }

  public static BooleanTerm of(final Boolean value) {
    return new BooleanTermImpl(value);
  }

  public static DecimalTerm of(final BigDecimal value, final SqlType sqlType) {
    return new DecimalTermImpl(value, sqlType);
  }

  public static DoubleTerm of(final Double value) {
    return new DoubleTermImpl(value);
  }

  public static IntegerTerm of(final Integer value) {
    return new IntegerTermImpl(value);
  }

  public static LongTerm of(final Long value) {
    return new LongTermImpl(value);
  }

  public static StringTerm of(final String value) {
    return new StringTermImpl(value);
  }

  public static TimestampTerm of(final Timestamp value) {
    return new TimestampTermImpl(value);
  }

  public static NullTerm ofNull() {
    return new NullTerm();
  }

  public static class NullTerm implements Term {

    public NullTerm() {
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return null;
    }

    @Override
    public SqlType getSqlType() {
      return null;
    }
  }

  public static class BooleanTermImpl implements BooleanTerm {

    private final Boolean value;

    public BooleanTermImpl(final Boolean value) {
      this.value = value;
    }

    public Boolean getBoolean(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BOOLEAN;
    }
  }


  public static class DoubleTermImpl implements DoubleTerm {

    private final Double value;

    public DoubleTermImpl(final Double value) {
      this.value = value;
    }

    public Double getDouble(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return getDouble(context);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.DOUBLE;
    }
  }

  public static class IntegerTermImpl implements IntegerTerm {

    private final Integer value;

    public IntegerTermImpl(final Integer value) {
      this.value = value;
    }

    public Integer getInteger(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return getInteger(context);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.INTEGER;
    }
  }

  public static class LongTermImpl implements LongTerm {

    private final Long value;

    public LongTermImpl(final Long value) {
      this.value = value;
    }

    public Long getLong(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return getLong(context);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BIGINT;
    }
  }

  public static class StringTermImpl implements StringTerm {

    private final String value;

    public StringTermImpl(final String value) {
      this.value = value;
    }

    public String getString(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return getString(context);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.STRING;
    }
  }

  public static class DecimalTermImpl implements DecimalTerm {

    private final BigDecimal value;
    private final SqlType sqlType;

    public DecimalTermImpl(final BigDecimal value, final SqlType sqlType) {
      this.value = value;
      this.sqlType = sqlType;
    }

    public BigDecimal getDecimal(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return getDecimal(context);
    }

    @Override
    public SqlType getSqlType() {
      return sqlType;
    }
  }

  public static class TimestampTermImpl implements TimestampTerm {

    private final long timeMs;

    public TimestampTermImpl(final Timestamp timestamp) {
      this.timeMs = timestamp.getTime();
    }

    public Timestamp getTimestamp(final TermEvaluationContext context) {
      return new Timestamp(timeMs);
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return getTimestamp(context);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.TIMESTAMP;
    }
  }
}
