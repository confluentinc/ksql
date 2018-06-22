/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "mask_keep_left", author = "Confluent",
    description = "Returns a version of the input string with all but the"
        + " specified number of left-most characters masked out."
        + " Default masking rules will replace all upper-case characters with 'X', all lower-case"
        + " characters with 'x', all digits with 'n', and any other character with '-'.")
public class MaskKeepLeftKudf {

  @Udf(description = "Returns a masked version of the input string. All characters except for the"
      + " first n will be replaced according to the default masking rules.")
  public String mask(final String input, final int numChars) {
    return doMask(new Masker(), input, numChars);
  }

  @Udf(description = "Returns a masked version of the input string. All characters except for the"
      + " first n will be replaced with the specified masking characters: e.g."
      + " mask_keep_left(input, numberToKeep, upperCaseMask, lowerCaseMask, digitMask, otherMask)"
      + " . Pass NULL for any of the mask characters to prevent masking of that character type.")
  public String mask(final String input, final int numChars, final String upper, final String lower,
      final String digit, final String other) {
    // TODO once KSQL gains Char sql-datatype support we should change the xxMask params to int
    // (codepoint) instead of String

    // TODO really need a way for UDFs to do one-shot init() stuff instead of repeating all this
    // literal-param manipulation and validation for every single record
    final int upperMask = upper == null ? Masker.NO_MASK : upper.codePointAt(0);
    final int lowerMask = lower == null ? Masker.NO_MASK : lower.codePointAt(0);
    final int digitMask = digit == null ? Masker.NO_MASK : digit.codePointAt(0);
    final int otherMask = other == null ? Masker.NO_MASK : other.codePointAt(0);
    final Masker masker = new Masker(upperMask, lowerMask, digitMask, otherMask);
    return doMask(masker, input, numChars);
  }

  private String doMask(final Masker masker, final String input, final int numChars) {
    validateParams(numChars);
    if (input == null) {
      return null;
    }
    final StringBuilder output = new StringBuilder(input.length());
    final int charsToKeep = Math.min(numChars, input.length());
    output.append(input.substring(0, charsToKeep));
    output.append(masker.mask(input.substring(charsToKeep)));
    return output.toString();
  }

  private void validateParams(final int numChars) {
    if (numChars < 0) {
      throw new KsqlFunctionException(
          "mask_keep_left requires a non-negative number of characters not to mask");
    }
  }
}
