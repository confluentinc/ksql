/*
 * Copyright 2018 Confluent Inc.
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
 */

package ${package};

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "reverse", description = "Example UDF that reverses an object")
public class ReverseUdf {

  @Udf(description = "Reverse a string")
  public String reverseString(String source) {
    return new StringBuilder(source).reverse().toString();
  }

  @Udf(description = "Reverse an integer")
  public String reverseInt(Integer source) {
    return new StringBuilder(source.toString()).reverse().toString();
  }

  @Udf(description = "Reverse a long")
  public String reverseLong(Long source) {
    return new StringBuilder(source.toString()).reverse().toString();
  }

  @Udf(description = "Reverse a double")
  public String reverseDouble(Double source) {
    return new StringBuilder(source.toString()).reverse().toString();
  }
}
