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
import avro.shaded.com.google.common.collect.Maps;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "arrays_to_map", author = "Confluent", description = "blah.")
public class ArraysToMapKudf {

  @Udf(description = "blah blah.")
  public Map<String, Object> map(final List<String> inputKeys, final List<Object> inputValues) {
    if (inputKeys == null || inputValues == null) {
      return null;
    }
    if (inputKeys.size() != inputValues.size()) {
      throw new KsqlFunctionException("input key and value arrays must be of equal length");
    }
    Map<String, Object> outputMap = Maps.newHashMapWithExpectedSize(inputKeys.size());
    for (int i = 0; i < inputKeys.size(); i++) {
      String thisKey = inputKeys.get(i);
      Object thisValue = inputValues.get(i);
      if (thisValue != null) {
        outputMap.put(inputKeys.get(i), thisValue);
      } else {
        outputMap.put(thisKey, null);
      }
    }
    return outputMap;
  }
}
