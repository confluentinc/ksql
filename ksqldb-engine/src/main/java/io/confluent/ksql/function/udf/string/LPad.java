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
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(
    name = "LPad",
    category = FunctionCategory.STRING,
    description = "Pads the input string, beginning from the left, with the specified padding"
        + " string until the target length is reached. If the input string is longer than the"
        + " specified target length it will be truncated. If the padding string is empty or"
        + " NULL, or the target length is negative, then NULL is returned.")
public class LPad {

  @Udf
  public String lpad(
      @UdfParameter(description = "String to be padded") final String input,
      @UdfParameter(description = "Target length") final Integer targetLen,
      @UdfParameter(description = "Padding string") final String padding) {

    if (input == null) {
      return null;
    }
    if (padding == null || padding.isEmpty() || targetLen == null || targetLen < 0) {
      return null;
    }
    final StringBuilder sb = new StringBuilder(targetLen + padding.length());
    final int padUpTo = Math.max(targetLen - input.length(), 0);
    for (int i = 0; i < padUpTo; i += padding.length()) {
      sb.append(padding);
    }
    sb.setLength(padUpTo);
    sb.append(input);
    sb.setLength(targetLen);
    return sb.toString();
  }
}
