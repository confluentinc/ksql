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

import com.google.common.base.Splitter;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.BytesUtils;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@UdfDescription(
    name = Split.NAME,
    category = FunctionCategory.STRING,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Splits a string or bytes into an array of substrings or bytes based on a "
        + "delimiter. If the delimiter is found at the beginning of the string/bytes, end of the "
        + "string/bytes, or there are contiguous delimiters in the string/bytes, then empty "
        + "strings/bytes are added to the array. If the delimiter is not found, then the original "
        + "string or bytes is returned as the only element in the array. "
        + "If the delimiter is empty, then all characters or bytes in the string or bytes "
        + "are split."
)
public class Split {
  static final String NAME = "split";

  private static final Pattern EMPTY_DELIMITER = Pattern.compile("");

  @Udf(description = "Splits a string into an array of substrings based on a delimiter.")
  public List<String> split(
      @UdfParameter(
          description = "The string to be split. If NULL, then function returns NULL.")
      final String string,
      @UdfParameter(
          description = "The delimiter to split a string by. If NULL, then function returns NULL.")
      final String delimiter) {
    if (string == null || delimiter == null) {
      return null;
    }

    // Java split() accepts regular expressions as a delimiter, but the behavior of this UDF split()
    // is to accept only literal strings. This method uses Guava Splitter instead, which does not
    // accept any regex pattern. This is to avoid a confusion to users when splitting by regex
    // special characters, such as '.' and '|'.

    try {
      // Guava Splitter does not accept empty delimiters. Use the Java split() method instead.
      if (delimiter.isEmpty()) {
        return Arrays.asList(EMPTY_DELIMITER.split(string));
      } else {
        return Splitter.on(delimiter).splitToList(string);
      }
    } catch (final Exception e) {
      throw new KsqlFunctionException(
          String.format("Invalid delimiter '%s' in the split() function.", delimiter), e);
    }
  }

  @Udf(description = "Splits a byte array into an array of bytes values based on a delimiter.")
  public List<ByteBuffer> split(
      @UdfParameter(
          description = "The byte array to be split. If NULL, then function returns NULL.")
      final ByteBuffer bytes,
      @UdfParameter(
          description = "The delimiter to split the byte array by. If NULL, then function"
              + " returns NULL.")
      final ByteBuffer delimiter) {
    if (bytes == null || delimiter == null) {
      return null;
    }

    final byte[] byteArray = BytesUtils.getByteArray(bytes);
    final byte[] byteDelimiter = BytesUtils.getByteArray(delimiter);

    return BytesUtils.split(byteArray, byteDelimiter)
        .stream()
        .map(ByteBuffer::wrap)
        .collect(Collectors.toList());
  }
}
