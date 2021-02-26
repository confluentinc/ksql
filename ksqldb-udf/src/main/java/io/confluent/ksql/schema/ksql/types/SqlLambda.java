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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An internal object to track the input types and return types for a lambda
 * argument that's seen as an argument inside of a UDF.
 */
@Immutable
public final class SqlLambda {

  private List<SqlType> inputTypes;
  private SqlType returnType;
  private final Integer numInputs;
  private boolean isFake;

  public static SqlLambda of(
      final List<SqlType> inputType,
      final SqlType returnType
  ) {
    return new SqlLambda(inputType, returnType);
  }

  public static SqlLambda newof(
      final Integer numInputs
  ) {
    return new SqlLambda(numInputs);
  }

  public SqlLambda(
      final List<SqlType> inputTypes,
      final SqlType returnType
  ) {
    this.inputTypes = ImmutableList.copyOf(requireNonNull(inputTypes, "inputType"));
    this.returnType = requireNonNull(returnType, "returnType");
    this.numInputs = inputTypes.size();
    this.isFake = true;
  }

  public SqlLambda(
      final Integer numInputs
  ) {
    this.inputTypes = null;
    this.returnType = null;
    this.numInputs = numInputs;
    this.isFake = true;
  }

  public void reverseIsFake() {this.isFake = false;}

  public boolean isNotImportant() {return isFake;}

  public List<SqlType> getInputType() {
    return inputTypes;
  }

  public SqlType getReturnType() {
    return returnType;
  }

  public Integer getNumInputs() {
    return numInputs;
  }

  public void setInputType(final List<SqlType> list) {
    this.inputTypes = list;
  }


  public void setReturnType(final SqlType type) {
    this.returnType = type;
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
    return Objects.equals(inputTypes, lambda.inputTypes)
        && Objects.equals(returnType, lambda.returnType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inputTypes, returnType);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    /*return "LAMBDA "
        + inputTypes.stream()
        .map(Object::toString)
        .collect(Collectors.joining(", ", "(", ")"))
        + " => "
        + returnType.toString(formatOptions);*/
    return isFake ? "LAMBDA" : "LAMBDA "
        + inputTypes.stream()
        .map(Object::toString)
        .collect(Collectors.joining(", ", "(", ")"))
        + " => "
        + returnType.toString(formatOptions);
  }
}
