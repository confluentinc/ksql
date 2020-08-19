/*
 * Copyright 2020 Confluent Inc.
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
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.regex.PatternSyntaxException;

@UdfDescription(name = "regexp_replace",
    author = KsqlConstants.CONFLUENT_AUTHOR,
    category = FunctionCategory.REGULAR_EXPRESSION,
    description = "Replaces all matches of a regexp in a string with a new substring.")
public class RegexpReplace {

  @Udf(description = "Returns a new string with all matches of regexp in str replaced with newStr")
  public String regexpReplace(
      @UdfParameter(
          description = "The source string. If null, then function returns null.") final String str,
      @UdfParameter(
          description = "The regexp to match."
              + " If null, then function returns null.") final String regexp,
      @UdfParameter(
          description = "The string to replace the matches with."
              + " If null, then function returns null.") final String newStr) {
    if (str == null || regexp == null || newStr == null) {
      return null;
    }

    try {
      return str.replaceAll(regexp, newStr);
    } catch (PatternSyntaxException e) {
      throw new KsqlFunctionException("Invalid regular expression pattern: " + regexp, e);
    }
  }
}
