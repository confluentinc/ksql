/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.ksql.function.udf.map;

import java.util.Map;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "map_concat", author = "Confluent",
    description = "Returns the union of the specified maps. If a given key is present in more"
        + " than one input map then the value for that key in the output map will be from the"
        + " last input map where it is found.")
public class MapUnionKudf {

  @SuppressWarnings("rawtypes")
  @Udf(description = "As above.")
  public Map union(final Map<String, Object> input1, final Map<String, Object> input2) {
    // TODO once UDF framework supports varargs, extend this method to take 1..n input maps
    if (input1 == null || input2 == null) {
      return null;
    }
    input1.putAll(input2);
    return input1;
  }

}
