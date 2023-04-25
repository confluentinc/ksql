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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public final class LiteralTerms {

  private LiteralTerms() {

  }

  public static Term of(final Boolean value) {
    return new BooleanTermImpl(value);
  }

  public static Term of(final BigDecimal value, final SqlType sqlType) {
    return new DecimalTermImpl(value, sqlType);
  }

  public static Term of(final Double value) {
    return new DoubleTermImpl(value);
  }

  public static Term of(final Integer value) {
    return new IntegerTermImpl(value);
  }

  public static Term of(final Long value) {
    return new LongTermImpl(value);
  }

  public static Term of(final String value) {
    return new StringTermImpl(value);
  }

  public static Term of(final Timestamp value) {
    return new TimestampTermImpl(value);
  }

  public static Term of(final Time value) {
    return new TimeTermImpl(value);
  }

  public static Term of(final Date value) {
    return new DateTermImpl(value);
  }

  public static Term of(final TimeUnit value) {
    return new IntervalUnitTermImpl(value);
  }

  public static Term of(final ByteBuffer value) {
    return new BytesTermImpl(value);
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

  public static class BooleanTermImpl implements Term {

    private final Boolean value;

    public BooleanTermImpl(final Boolean value) {
      this.value = value;
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


  public static class DoubleTermImpl implements Term {

    private final Double value;

    public DoubleTermImpl(final Double value) {
      this.value = value;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.DOUBLE;
    }
  }

  public static class IntegerTermImpl implements Term {

    private final Integer value;

    public IntegerTermImpl(final Integer value) {
      this.value = value;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.INTEGER;
    }
  }

  public static class LongTermImpl implements Term {

    private final Long value;

    public LongTermImpl(final Long value) {
      this.value = value;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BIGINT;
    }
  }

  public static class StringTermImpl implements Term {

    private final String value;

    public StringTermImpl(final String value) {
      this.value = value;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.STRING;
    }
  }

  public static class DecimalTermImpl implements Term {

    private final BigDecimal value;
    private final SqlType sqlType;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public DecimalTermImpl(final BigDecimal value, final SqlType sqlType) {
      this.value = value;
      this.sqlType = sqlType;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    @Override
    public Object getValue(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public SqlType getSqlType() {
      return sqlType;
    }
  }

  public static class TimestampTermImpl implements Term {

    private final long timeMs;

    public TimestampTermImpl(final Timestamp timestamp) {
      this.timeMs = timestamp.getTime();
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return new Timestamp(timeMs);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.TIMESTAMP;
    }
  }

  public static class TimeTermImpl implements Term {

    private final long timeMs;

    public TimeTermImpl(final Time time) {
      this.timeMs = time.getTime();
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return new Time(timeMs);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.TIME;
    }
  }

  public static class DateTermImpl implements Term {

    private final long dateMs;

    public DateTermImpl(final Date date) {
      this.dateMs = date.getTime();
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return new Date(dateMs);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.DATE;
    }
  }

  public static class BytesTermImpl implements Term {

    private final ByteBuffer value;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public BytesTermImpl(final ByteBuffer bytes) {
      this.value = bytes;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    @Override
    public Object getValue(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BYTES;
    }
  }

  public static class IntervalUnitTermImpl implements Term {

    private final TimeUnit value;

    public IntervalUnitTermImpl(final TimeUnit timeUnit) {
      this.value = timeUnit;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return value;
    }

    @Override
    public SqlType getSqlType() {
      return null;
    }
  }
}
