/*
 * Copyright 2018 Confluent Inc.
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
    name = "substring",
    category = FunctionCategory.STRING,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Returns a substring of the passed in value."
)
public class Substring {

  @Udf(description = "Returns a substring of str from pos to the end of str")
  public String substring(
      @UdfParameter(description = "The source string.") final String str,
      @UdfParameter(description = "The base-one position to start from.") final Integer pos
  ) {
    if (str == null || pos == null) {
      return null;
    }
    final int start = getStartIndex(str, pos);
    return str.substring(start);
  }

  @Udf(description = "Returns a substring of str that starts at pos and is of length len")
  public String substring(
      @UdfParameter(description = "The source string.") final String str,
      @UdfParameter(description = "The base-one position to start from.") final Integer pos,
      @UdfParameter(description = "The length to extract.") final Integer length
  ) {
    if (str == null || pos == null || length == null) {
      return null;
    }
    final int start = getStartIndex(str, pos);
    final int end = getEndIndex(str, start, length);
    return str.substring(start, end);
  }

  private static int getStartIndex(final String value, final Integer pos) {
    return pos < 0
        ? Math.max(value.length() + pos, 0)
        : Math.max(Math.min(pos - 1, value.length()), 0);
  }

  private static int getEndIndex(final String value, final int start, final int length) {
    return Math.max(Math.min(start + length, value.length()), start);
  }
}
