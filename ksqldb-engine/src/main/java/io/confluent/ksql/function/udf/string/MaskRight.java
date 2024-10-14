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

@UdfDescription(
    name = MaskRight.NAME,
    category = FunctionCategory.STRING,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Returns a version of the input string with the"
        + " specified number of characters, counting back from the end of the string, masked out."
        + " Default masking rules will replace all upper-case characters with 'X', all lower-case"
        + " characters with 'x', all digits with 'n', and any other character with '-'."
)
public class MaskRight {

  protected static final String NAME = "mask_right";

  @Udf(description = "Returns a masked version of the input string. The last n characters"
      + " will be replaced according to the default masking rules.")
  @SuppressWarnings("MethodMayBeStatic") // Invoked via reflection
  public String mask(
      @UdfParameter("input STRING to be masked") final String input,
      @UdfParameter("number of characters to mask before the end") final int numChars
  ) {
    return doMask(new Masker(), input, numChars);
  }

  @Udf(description = "Returns a masked version of the input string. The last n characters"
      + " will be replaced with the specified masking characters")
  @SuppressWarnings("MethodMayBeStatic") // Invoked via reflection
  public String mask(
      @UdfParameter("input STRING to be masked") final String input,
      @UdfParameter("number of characters to mask before the end") final int numChars,
      @UdfParameter("upper-case mask, or NULL to use default") final String upper,
      @UdfParameter("lower-case mask, or NULL to use default") final String lower,
      @UdfParameter("digit mask, or NULL to use default") final String digit,
      @UdfParameter("mask for other characters, or NULL to use default") final String other
  ) {
    final int upperMask = Masker.getMaskCharacter(upper);
    final int lowerMask = Masker.getMaskCharacter(lower);
    final int digitMask = Masker.getMaskCharacter(digit);
    final int otherMask = Masker.getMaskCharacter(other);
    final Masker masker = new Masker(upperMask, lowerMask, digitMask, otherMask);
    return doMask(masker, input, numChars);
  }

  private static String doMask(final Masker masker, final String input, final int numChars) {
    Masker.validateParams(NAME, numChars);
    if (input == null) {
      return null;
    }
    final StringBuilder output = new StringBuilder(input.length());
    final int charsToKeep = Math.max(0, input.length() - numChars);
    output.append(input, 0, charsToKeep);
    output.append(masker.mask(input.substring(charsToKeep)));
    return output.toString();
  }
}
