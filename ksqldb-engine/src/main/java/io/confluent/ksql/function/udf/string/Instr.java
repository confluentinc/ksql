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

import static org.apache.commons.lang3.StringUtils.ordinalIndexOf;
import static org.apache.commons.lang3.StringUtils.substring;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.util.KsqlConstants;
import org.apache.commons.lang3.StringUtils;

@UdfDescription(
    name = "instr",
    category = FunctionCategory.STRING,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Returns the position of substring in the provided string"
)
public class Instr {

  @Udf
  public int instr(final String str, final String substring) {
    return instr(str, substring, 1);
  }

  @Udf
  public int instr(final String str, final String substring, final int position) {
    return instr(str, substring, position, 1);
  }

  @Udf
  public int instr(final String str, final String substring, final int position,
      final int occurrence) {
    if (str == null || substring == null) {
      return 0;
    }
    if (Math.abs(position) > str.length()) {
      return 0;
    }
    if (position < 0) {
      final String reversedStr = StringUtils.reverse(str);
      final String reversedSubstring = StringUtils.reverse(substring);
      return find(reversedStr, reversedSubstring, position * -1, occurrence, true);
    }
    return find(str, substring, position, occurrence, false);
  }

  private int find(final String str, final String substring, final int position,
      final int occurence, final boolean reversed) {
    final int i = ordinalIndexOf(substring(str, position - 1), substring, occurence);
    if (i == -1) {
      return 0;
    }
    return reversed ? str.length() - position - i - substring.length() + 2 : i + position;
  }
}
