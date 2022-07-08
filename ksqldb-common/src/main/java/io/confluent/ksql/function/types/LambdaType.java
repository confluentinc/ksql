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

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class LambdaType extends ObjectType {

  private final ImmutableList<ParamType> inputTypes;
  private final ParamType returnType;

  private LambdaType(
      final List<ParamType> inputTypes,
      final ParamType returnType
  ) {
    this.inputTypes = ImmutableList.copyOf(
        Objects.requireNonNull(inputTypes, "inputTypes"));
    this.returnType = Objects.requireNonNull(returnType, "returnType");
  }

  public static LambdaType of(
      final List<ParamType> inputTypes,
      final ParamType returnType
  ) {
    return new LambdaType(inputTypes, returnType);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "inputTypes is ImmutableList")
  public List<ParamType> inputTypes() {
    return inputTypes;
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
    final LambdaType lambdaType = (LambdaType) o;
    return Objects.equals(inputTypes, lambdaType.inputTypes)
        && Objects.equals(returnType, lambdaType.returnType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inputTypes, returnType);
  }

  @Override
  public String toString() {
    return "LAMBDA "
        + inputTypes.stream()
        .map(Object::toString)
        .collect(Collectors.joining(", ", "(", ")"))
        + " => "
        + returnType;
  }
}
