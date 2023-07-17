/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@UdfDescription(
    name = "concat_ws",
    category = FunctionCategory.STRING,
    description = "Concatenate several strings or bytes, inserting a separator passed as the "
        + "first argument between each one.")
public class ConcatWS {

  private static final Concat CONCAT = new Concat();

  @Udf
  public String concatWS(
      @UdfParameter(description = "Separator string and values to join") final String... inputs) {
    if (inputs == null || inputs.length < 2) {
      throw new KsqlFunctionException("Function Concat_WS expects at least two input arguments.");
    }

    final String separator = inputs[0];
    if (separator == null) {
      return null;
    }

    return Arrays.stream(inputs, 1,
        inputs.length)
        .filter(Objects::nonNull)
        .collect(Collectors.joining(separator));
  }

  @Udf
  public ByteBuffer concatWS(
      @UdfParameter(description = "Separator and bytes values to join")
      final ByteBuffer... inputs) {
    if (inputs == null || inputs.length < 2) {
      throw new KsqlFunctionException("Function Concat_WS expects at least two input arguments.");
    }

    final ByteBuffer separator = inputs[0];
    if (separator == null) {
      return null;
    }

    final List<ByteBuffer> concatInputs = new ArrayList<>();
    for (int i = 1; i < inputs.length; i++) {
      if (Objects.nonNull(inputs[i])) {
        if (concatInputs.size() != 0) {
          concatInputs.add(separator.duplicate());
        }
        concatInputs.add(inputs[i]);
      }
    }

    return CONCAT.concat(concatInputs.toArray(new ByteBuffer[0]));
  }
}
