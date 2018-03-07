/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

public class SubstringKudf implements Kudf {

  @Override
  public Object evaluate(Object... args) {
    if ((args.length < 2) || (args.length > 3)) {
      throw new KsqlFunctionException("Substring udf should have two or three input argument.");
    }
    String string = args[0].toString();
    long start = (Long) args[1];
    if (args.length == 2) {
      return string.substring((int) start);
    } else {
      long end = (Long) args[2];
      return string.substring((int) start, (int) end);
    }
  }
}
