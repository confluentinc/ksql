/*
 * Copyright 2019 Confluent Inc.
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
import io.confluent.ksql.util.BytesUtils;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@SuppressWarnings("unused") // Invoked via reflection.
@UdfDescription(
    name = "replace",
    category = FunctionCategory.STRING,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Replaces all occurences of a substring in a string with a new substring."
)
public class Replace {
  @Udf(description = "Returns a new string with all occurences of oldStr in str with newStr")
  public String replace(
      @UdfParameter(
          description = "The source string. If null, then function returns null.") final String str,
      @UdfParameter(
          description = "The substring to replace."
              + " If null, then function returns null.") final String oldStr,
      @UdfParameter(
          description = "The string to replace the old substrings with."
              + " If null, then function returns null.") final String newStr) {
    if (str == null || oldStr == null || newStr == null) {
      return null;
    }

    return str.replace(oldStr, newStr);
  }

  @Udf
  public ByteBuffer replace(
      @UdfParameter(
          description = "The source bytes."
              + " If null, then function returns null.") final ByteBuffer source,
      @UdfParameter(
          description = "The byte subset to replace."
              + " If null, then function returns null.") final ByteBuffer toReplace,
      @UdfParameter(
          description = "The bytes to replace the old bytes with."
              + " If null, then function returns null.") final ByteBuffer replaceWith) {

    if (source == null || toReplace == null || replaceWith == null) {
      return null;
    }

    //if replaceWith.length == toReplace.length, DON'T create a new array
    //else, create an arraylist. Create another arraylist2 and a value matchingBytesNum.
    //populate arraylist2 as long as the next byte matches the next byte in our toReplace[matchingBytesNum]
    //if matchingBytesNum reaches the length of toReplace, we have a matching subbytes and append
    //to our 1st arraylist the replaceWith bytes. If they diverge, just dump the original arraylist
    final byte[] sourceArr = BytesUtils.getByteArray(source);
    final byte[] toReplaceArr = BytesUtils.getByteArray(toReplace);
    final byte[] replaceWithArr = BytesUtils.getByteArray(replaceWith);


    final String sourceString =
        new String(sourceArr, 0, sourceArr.length, StandardCharsets.US_ASCII);
    final String toReplaceString =
        new String(toReplaceArr, 0, toReplaceArr.length, StandardCharsets.US_ASCII);
    final String replaceWithString
        = new String(replaceWithArr, 0, replaceWithArr.length, StandardCharsets.US_ASCII);


    final String replacedString = sourceString.replace(toReplaceString, replaceWithString);

    final byte[] replacedStringBytes = replacedString.getBytes(StandardCharsets.US_ASCII);

    return ByteBuffer.wrap(replacedStringBytes);

  }
}
