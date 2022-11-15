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
import org.apache.commons.lang3.StringEscapeUtils;

@UdfDescription(
    name = "Chr",
    category = FunctionCategory.STRING,
    description = "Returns a single-character string corresponding to the input character code.")
public class Chr {

  @Udf
  public String chr(@UdfParameter(
      description = "Decimal codepoint") final Integer decimalCode) {
    if (decimalCode == null) {
      return null;
    }
    if (!Character.isValidCodePoint(decimalCode)) {
      return null;
    }
    final char[] resultChars = Character.toChars(decimalCode);
    return String.valueOf(resultChars);
  }

  @Udf
  public String chr(@UdfParameter(
      description = "UTF16 code for the desired character e.g. '\\u004b'") final String utf16Code) {
    if (utf16Code == null || utf16Code.length() < 6 || !utf16Code.startsWith("\\u")) {
      return null;
    }
    return StringEscapeUtils.unescapeJava(utf16Code);
  }
}
