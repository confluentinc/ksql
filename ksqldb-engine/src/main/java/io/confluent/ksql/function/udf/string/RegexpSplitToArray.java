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

import com.google.common.base.Splitter;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@UdfDescription(name = "regexp_split_to_array",
    author = KsqlConstants.CONFLUENT_AUTHOR,
    category = FunctionCategory.REGULAR_EXPRESSION,
    description = "Splits a string into an array of substrings based on a regexp. "
        + "If the regexp is found at the beginning of the string, end of the string, or there "
        + "are contiguous matches in the string, then empty strings are added to the array. "
        + "If the regexp is not found, then the original string is returned as the only "
        + "element in the array. If the regexp is empty, then all characters in the string are "
        + "split.")
public class RegexpSplitToArray {

  @Udf(description = "Splits a string into an array of substrings based on a regexp.")
  public List<String> regexpSplit(
      @UdfParameter(
          description = "The string to be split. If NULL, then function returns NULL.")
      final String string,
      @UdfParameter(
          description = "The regular expression to split the string by. "
              + "If NULL, then function returns NULL.")
      final String regexp) {
    if (string == null || regexp == null) {
      return null;
    }

    // Use Guava version to be compatible with other splitting functions.
    final Pattern p = getPattern(regexp);
    if (regexp.isEmpty() || p.matcher("").matches()) {
      return Arrays.asList(p.split(string));
    } else {
      return Splitter.on(p).splitToList(string);
    }
  }

  private Pattern getPattern(final String regexp) {
    try {
      return Pattern.compile(regexp);
    } catch (PatternSyntaxException e) {
      throw new KsqlFunctionException("Invalid regular expression pattern: " + regexp, e);
    }
  }
}
