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

package io.confluent.ksql.schema.ksql.types;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

/**
 * An internal object to represent a lambda function, this class only
 * has information on the number of input arguments for a lambda.
 */
@Immutable
public class SqlLambda {

  private final int numInputs;

  public static SqlLambda of(
      final Integer numInputs
  ) {
    return new SqlLambda(numInputs);
  }

  @VisibleForTesting
  SqlLambda(
      final Integer numInputs
  ) {
    this.numInputs = numInputs;
  }

  public Integer getNumInputs() {
    return numInputs;
  }


  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlLambda lambda = (SqlLambda) o;
    return numInputs == lambda.numInputs ;
  }

  @Override
  public int hashCode() {
    return Objects.hash(numInputs);
  }

  @Override
  public String toString() {
    return "LAMBDA";
  }
}
