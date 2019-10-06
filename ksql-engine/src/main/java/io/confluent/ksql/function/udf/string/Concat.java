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

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

@UdfDescription(name = "Concat", description = Concat.DESCRIPTION)
public class Concat {

  static final String DESCRIPTION = "Returns a new string that is the concatenation of the "
      + "arguments.";

  @Udf
  public String concat(@UdfParameter final Object... args) {
    if (args.length < 2) {
      throw new KsqlFunctionException("Concat should have at least two input argument.");
    }

    return Arrays.stream(args)
        .filter(Objects::nonNull)
        .map(Object::toString)
        .collect(Collectors.joining());
  }
}
