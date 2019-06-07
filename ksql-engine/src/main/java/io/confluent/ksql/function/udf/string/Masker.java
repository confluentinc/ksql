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

class Masker {

  private static final int DEFAULT_UPPERCASE_MASK = 'X';
  private static final int DEFAULT_LOWERCASE_MASK = 'x';
  private static final int DEFAULT_DIGIT_MASK = 'n';
  private static final int DEFAULT_OTHER_MASK = '-';
  // safe to use MAX_VALUE because codepoints use only the lower 21 bits of an int
  private static final int NO_MASK = Integer.MAX_VALUE;

  private int upperMask = DEFAULT_UPPERCASE_MASK;
  private int lowerMask = DEFAULT_LOWERCASE_MASK;
  private int digitMask = DEFAULT_DIGIT_MASK;
  private int otherMask = DEFAULT_OTHER_MASK;

  Masker(final int upperMask, final int lowerMask, final int digitMask, final int otherMask) {
    this.upperMask = upperMask;
    this.lowerMask = lowerMask;
    this.digitMask = digitMask;
    this.otherMask = otherMask;
  }

  Masker() {}

  public String mask(final String input) {
    final StringBuilder output = new StringBuilder(input.length());
    for (int i = 0; i < input.length(); i++) {
      output.appendCodePoint(maskCharacter(input.codePointAt(i)));
    }
    return output.toString();
  }

  private int maskCharacter(final int c) {
    switch (Character.getType(c)) {
      case Character.UPPERCASE_LETTER:
        if (upperMask != NO_MASK) {
          return upperMask;
        }
        break;
      case Character.LOWERCASE_LETTER:
        if (lowerMask != NO_MASK) {
          return lowerMask;
        }
        break;
      case Character.DECIMAL_DIGIT_NUMBER:
        if (digitMask != NO_MASK) {
          return digitMask;
        }
        break;
      default:
        if (otherMask != NO_MASK) {
          return otherMask;
        }
        break;
    }
    return c;
  }

  static int getMaskCharacter(final String stringMask) {
    return stringMask == null ? NO_MASK : stringMask.codePointAt(0);
  }
}
