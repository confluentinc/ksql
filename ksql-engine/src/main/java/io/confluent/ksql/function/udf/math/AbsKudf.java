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

package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

import java.util.HashMap;
import java.util.Map;

public class AbsKudf implements Kudf {

  Map<Class, Abs> funcs = new HashMap<>();

  public AbsKudf(){
    init();
  }
  @Override
  public void init() {
    funcs.put(Double.class, val -> Math.abs(val.doubleValue()));
    funcs.put(Long.class, val -> Math.abs(val.longValue()));
    funcs.put(Integer.class, val -> Math.abs(val.intValue()));
    funcs.put(Float.class, val -> Math.abs(val.floatValue()));
    funcs.put(Short.class, val -> Math.abs(val.shortValue()));
  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 1) {
      throw new KsqlFunctionException("Abs udf should have one input argument.");
    }

    if (args[0] instanceof Number) {
      return funcs.get(args[0].getClass()).abs((Number) args[0]);
    } else {
      throw new RuntimeException(String.format("Value: %s is not a Number"));
    }
  }

  private interface Abs<T extends Number> {
    T abs(Number val);
  }
}
