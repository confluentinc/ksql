/*
 * Copyright 2019 Confluent Inc.
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
import io.confluent.ksql.util.KsqlConstants;

@SuppressWarnings("unused") // Invoked via reflection.
@UdfDescription(
    name = "replace",
    category = FunctionCategory.STRING,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Replaces all occurances of a substring in a string with a new substring."
)
public class Replace {
  @Udf(description = "Returns a new string with all occurences of oldStr in str with newStr")
  public String replace(
      @UdfParameter(
          description = "The source string. If null, then function returns null.") final String str,
      @UdfParameter(
          description = "The substring to replace."
              + " If null, then function returns null.") final String oldStr,
      @UdfParameter(
          description = "The string to replace the old substrings with."
              + " If null, then function returns null.") final String newStr) {
    if (str == null || oldStr == null || newStr == null) {
      return null;
    }

    return str.replace(oldStr, newStr);
  }
}
