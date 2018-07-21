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

import java.util.List;
import java.util.Map;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "element", author = "Confluent",
    description = "Returns the specified element from a map or array.")
public class ElementKudf {

  @Udf(description = "Returns the number of keys in the specified map.")
  public Object element(final Map<String, ?> inputMap, String key) {
    if (inputMap == null) {
      return null;
    }
    return inputMap.get(key);
  }

  @SuppressWarnings("rawtypes")
  @Udf(description = "Returns the element at the specified index (counting from 0) in an array.")
  public Object element(final List inputList, int index) {
    if (inputList == null || index < 0 || index >= inputList.size()) {
      return null;
    }
    return inputList.get(index);
  }
}
