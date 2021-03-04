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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An internal object to track the input types and return types for a lambda
 * function.
 */
@Immutable
public final class SqlLambdaResolved extends SqlLambda {

  private final ImmutableList<SqlType> inputTypes;
  private final SqlType returnType;

  public static SqlLambdaResolved of(
      final int numInputs
  ) {
    throw new IllegalArgumentException("SqlLambdaResolved can only be constructed " 
        + "with the input types and return type specified.");
  }

  public static SqlLambdaResolved of(
      final List<SqlType> inputType,
      final SqlType returnType
  ) {
    return new SqlLambdaResolved(inputType, returnType);
  }

  @VisibleForTesting
  SqlLambdaResolved(
      final List<SqlType> inputTypes,
      final SqlType returnType
  ) {
    super(inputTypes.size());
    this.inputTypes = ImmutableList.copyOf(requireNonNull(inputTypes, "inputType"));
    this.returnType = requireNonNull(returnType, "returnType");
  }

  public List<SqlType> getInputType() {
    return inputTypes;
  }

  public SqlType getReturnType() {
    return returnType;
  }


  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlLambdaResolved lambda = (SqlLambdaResolved) o;
    return super.equals(o) && Objects.equals(inputTypes, lambda.inputTypes)
        && Objects.equals(returnType, lambda.returnType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), inputTypes, returnType);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    return "LAMBDA "
        + inputTypes.stream()
        .map(Object::toString)
        .collect(Collectors.joining(", ", "(", ")"))
        + " => "
        + returnType.toString(formatOptions);
  }
}
