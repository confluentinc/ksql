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

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "substring",
    author = "Confluent",
    description = "returns a substring of the passed in value")
public class Substring {

  @Udf(description = "Returns a string that is a substring of this string. The"
      + " substring begins with the character at the specified startIndex and"
      + " extends to the end of this string.")
  public String substring(final String value, final int startIndex) {
    return value.substring(startIndex);
  }

  @Udf(description = "Returns a string that is a substring of this string. The"
      + " substring begins with the character at the specified startIndex and"
      + " extends to the character at endIndex -1.")
  public String substring(final String value, final int startIndex, final int endIndex) {
    return value.substring(startIndex, endIndex);
  }

}
