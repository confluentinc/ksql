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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unused") // Invoked via reflection.
@UdfDescription(
    name = "initcap",
    category = FunctionCategory.STRING,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Capitalizes the first letter of each word in a string and the rest lowercased."
        + " Words are delimited by whitespace."
)
public class InitCap {
  @Udf(description = "Returns the string with the the first letter"
      + " of each word capitalized and the rest lowercased")
  public String initcap(
      @UdfParameter(
          description = "The source string."
              + " If null, then function returns null.") final String str) {
    if (str == null) {
      return null;
    }

    final Pattern pattern = Pattern.compile("[^\\s]+\\s*");
    final Matcher matcher = pattern.matcher(str.toLowerCase());
    String initCapped = "";
    while (matcher.find()) {
      final String part = matcher.group();
      initCapped = initCapped.concat(part.substring(0, 1).toUpperCase() + part.substring(1));
    }

    return initCapped;
  }
}
