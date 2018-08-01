/*
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
import io.confluent.ksql.function.udf.UdfParameter;

@SuppressWarnings("unused") // Invoked via reflection.
@UdfDescription(name = "substring",
    author = "Confluent",
    description = "Returns a substring of the passed in value")
public class Substring {

  @Udf(description = "Returns a substring of str that starts at pos "
      + " and continues to the end of the string")
  public String substring(
      @UdfParameter("value") final String str,
      @UdfParameter(value = "startIndex",
          description = "The base-one position the substring starts from.") final int pos) {
    final int start = getStartIndex(str, pos);
    return str.substring(start);
  }

  @Udf(description = "Returns a substring of str that starts at pos and is of length len")
  public String substring(
      @UdfParameter("value") final String str,
      @UdfParameter(value = "startIndex",
          description = "The base-one position the substring starts from.") final int pos,
      @UdfParameter(value = "len",
          description = "The length of the substring to extract.") final int len) {
    final int start = getStartIndex(str, pos);
    final int end = getEndIndex(str, start, len);
    return str.substring(start, end);
  }

  private static int getStartIndex(final String value, final int pos) {
    return pos < 0
        ? Math.max(value.length() + pos, 0)
        : Math.min(pos - 1, value.length());
  }

  private static int getEndIndex(final String value, final int start, final int length) {
    return Math.max(Math.min(start + length, value.length()), start);
  }
}