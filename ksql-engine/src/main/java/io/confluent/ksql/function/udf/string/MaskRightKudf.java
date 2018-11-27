/*
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

@UdfDescription(name = "mask_right", author = "Confluent",
    description = "Returns a version of the input string with the"
        + " specified number of characters, counting back from the end of the string, masked out."
        + " Default masking rules will replace all upper-case characters with 'X', all lower-case"
        + " characters with 'x', all digits with 'n', and any other character with '-'.")
public class MaskRightKudf {

  @Udf(description = "Returns a masked version of the input string. The last n characters"
      + " will be replaced according to the default masking rules.")
  public String mask(final String input, final int numChars) {
    return doMask(new Masker(), input, numChars);
  }

  @Udf(description = "Returns a masked version of the input string. The last n characters"
      + " will be replaced with the specified masking characters: e.g."
      + " mask_right(input, numberToMask, upperCaseMask, lowerCaseMask, digitMask, otherMask)"
      + " . Pass NULL for any of the mask characters to prevent masking of that character type.")
  public String mask(final String input, final int numChars, final String upper, final String lower,
      final String digit, final String other) {
    // TODO once KSQL gains Char sql-datatype support we should change the xxMask params to int
    // (codepoint) instead of String

    final int upperMask = Masker.getMaskCharacter(upper);
    final int lowerMask = Masker.getMaskCharacter(lower);
    final int digitMask = Masker.getMaskCharacter(digit);
    final int otherMask = Masker.getMaskCharacter(other);
    final Masker masker = new Masker(upperMask, lowerMask, digitMask, otherMask);
    return doMask(masker, input, numChars);
  }

  private String doMask(final Masker masker, final String input, final int numChars) {
    validateParams(numChars);
    if (input == null) {
      return null;
    }
    final StringBuilder output = new StringBuilder(input.length());
    final int charsToKeep = Math.max(0, input.length() - numChars);
    output.append(input.substring(0, charsToKeep));
    output.append(masker.mask(input.substring(charsToKeep)));
    return output.toString();
  }

  private void validateParams(final int numChars) {
    if (numChars < 0) {
      throw new KsqlFunctionException(
          "mask_right requires a non-negative number of characters to mask");
    }
  }
}
