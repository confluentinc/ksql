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
    name = "mask",
    category = FunctionCategory.STRING,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Returns a version of the input string with every character replaced by a mask."
        + " Default masking rules will replace all upper-case characters with 'X', all lower-case"
        + " characters with 'x', all digits with 'n', and any other character with '-'."
)
public class Mask {

  @Udf(description = "Returns a masked version of the input string. All characters of the input"
      + " will be replaced according to the default masking rules.")
  @SuppressWarnings("MethodMayBeStatic") // Invoked via reflection
  public String mask(
      @UdfParameter("input STRING to be masked") final String input
  ) {
    return doMask(new Masker(), input);
  }

  @Udf(description = "Returns a masked version of the input string. All characters of the input"
      + " will be replaced with the specified masking characters")
  @SuppressWarnings("MethodMayBeStatic") // Invoked via reflection
  public String mask(
      @UdfParameter("input STRING to be masked") final String input,
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
    return doMask(masker, input);
  }

  private static String doMask(final Masker masker, final String input) {
    if (input == null) {
      return null;
    }
    return masker.mask(input);
  }
}
