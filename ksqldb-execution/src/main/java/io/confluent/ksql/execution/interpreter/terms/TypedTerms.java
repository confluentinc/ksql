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
import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * These are interfaces for terms that evaluate to a particular type. These are useful if it's
 * preferable to bake in assumptions about types at term compile time to remove the need to
 * cast term inputs at evaluation time. This was done rather than making a generic class Term<T>
 * because the visiting pattern doesn't allow for Terms of differing generic types.
 */
public class TypedTerms {

  public interface TimestampTerm extends Term {
    Timestamp getTimestamp(TermEvaluationContext context);
  }

  public interface BooleanTerm extends Term {
    Boolean getBoolean(TermEvaluationContext context);
  }

  public interface DecimalTerm extends Term {
    BigDecimal getDecimal(TermEvaluationContext context);
  }

  public interface DoubleTerm extends Term {
    Double getDouble(TermEvaluationContext context);
  }

  public interface IntegerTerm extends Term {
    Integer getInteger(TermEvaluationContext context);
  }

  public interface LongTerm extends Term {
    Long getLong(TermEvaluationContext context);
  }

  public interface StringTerm extends Term {
    String getString(TermEvaluationContext context);
  }
}
