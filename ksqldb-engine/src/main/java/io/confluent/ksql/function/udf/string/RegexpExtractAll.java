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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@UdfDescription(
    name = "regexp_extract_all",
    category = FunctionCategory.REGULAR_EXPRESSION,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Extracts the all subtrings matched by a regex pattern")
public class RegexpExtractAll {

  @Udf(description = "Returns all substrings of the input that matches the given regex pattern")
  public List<String> regexpExtractAll(
      @UdfParameter(description = "The regex pattern") final String pattern,
      @UdfParameter(description = "The input string to apply regex on") final String input
  ) {
    return regexpExtractAll(pattern, input, 0);
  }

  @Udf(description = "Returns all substrings of the "
      + "input that matches the regex pattern and the capturing group number specified")
  public List<String> regexpExtractAll(
      @UdfParameter(description = "The regex pattern") final String pattern,
      @UdfParameter(description = "The input string to apply regex on") final String input,
      @UdfParameter(description = "The capturing group number") final Integer group
  ) {
    if (pattern == null || input == null || group == null) {
      return null;
    }

    final Pattern p = getPattern(pattern);
    final Matcher m = p.matcher(input);

    if (group > m.groupCount()) {
      return null;
    }

    final List<String> matches = new ArrayList<>();
    while (m.find()) {
      matches.add(m.group(group));
    }
    return matches;
  }

  private Pattern getPattern(final String regexp) {
    try {
      return Pattern.compile(regexp);
    } catch (PatternSyntaxException e) {
      throw new KsqlFunctionException("Invalid regular expression pattern: " + regexp, e);
    }
  }
}
