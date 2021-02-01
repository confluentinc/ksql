/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.types;

import java.util.Objects;

public final class KsqlLambdaType extends ObjectType {

  private final ParamType inputType;
  private final ParamType returnType;

  private KsqlLambdaType(
      final ParamType inputType,
      final ParamType returnType
  ) {
    this.inputType = inputType;
    this.returnType = returnType;
  }

  public static KsqlLambdaType of(
      final ParamType inputType,
      final ParamType returnType
  ) {
    return new KsqlLambdaType(inputType, returnType);
  }

  public ParamType inputType() {
    return inputType;
  }

  public ParamType returnType() {
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
    final KsqlLambdaType arrayType = (KsqlLambdaType) o;
    return Objects.equals(inputType, arrayType.inputType)
        && Objects.equals(returnType, arrayType.returnType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inputType);
  }

  @Override
  public String toString() {
    return "KSQL_LAMBDA<" + inputType + ", " + returnType + ">";
  }
}
