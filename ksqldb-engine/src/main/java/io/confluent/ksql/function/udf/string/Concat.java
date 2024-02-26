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

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

@UdfDescription(
    name = "concat",
    category = FunctionCategory.STRING,
    description = "Concatenate an arbitrary number of string or bytes fields together")
public class Concat {

  @Udf
  public String concat(@UdfParameter(
      description = "The varchar fields to concatenate") final String... inputs) {
    if (inputs == null) {
      return null;
    }

    return Arrays.stream(inputs)
        .filter(Objects::nonNull)
        .collect(Collectors.joining());
  }

  @Udf
  public ByteBuffer concat(@UdfParameter(
      description = "The bytes fields to concatenate") final ByteBuffer... inputs) {
    if (inputs == null) {
      return null;
    }

    int capacity = 0;

    for (final ByteBuffer bytes : inputs) {
      if (Objects.nonNull(bytes)) {
        capacity += bytes.capacity();
      }
    }

    final ByteBuffer concatenated = ByteBuffer.allocate(capacity);
    Arrays.stream(inputs)
        .filter(Objects::nonNull)
        .forEachOrdered(concatenated::put);

    concatenated.rewind();
    return concatenated;
  }

}
