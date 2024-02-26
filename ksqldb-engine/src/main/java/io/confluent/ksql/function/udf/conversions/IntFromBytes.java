/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.conversions;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.BytesUtils;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@UdfDescription(
    name = "int_from_bytes",
    category = FunctionCategory.CONVERSIONS,
    description = "Converts a BYTES value to an INT value according to the specified"
        + " byte order. BYTES must be 4 bytes long or a NULL value will be returned.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class IntFromBytes {
  private static final int BYTES_LENGTH = 4;

  @Udf(description = "Converts a BYTES value to an INT value using the 'BIG_ENDIAN' byte order."
      + " BYTES must be 4 bytes long or a NULL value will be returned.")
  public Integer intFromBytes(
      @UdfParameter(description = "The BYTES value to convert.")
      final ByteBuffer value
  ) {
    return intFromBytes(value, ByteOrder.BIG_ENDIAN);
  }

  @Udf(description = "Converts a BYTES value to an INT value according to the specified"
      + " byte order. BYTES must be 4 bytes long or a NULL value will be returned.")
  public Integer intFromBytes(
      @UdfParameter(description = "The BYTES value to convert.")
      final ByteBuffer value,
      @UdfParameter(description = "The byte order. Valid orders are 'BIG_ENDIAN' and"
          + " 'LITTLE_ENDIAN'. If omitted, 'BIG_ENDIAN' is used.")
      final String byteOrder
  ) {
    return intFromBytes(value, BytesUtils.byteOrderType(byteOrder));
  }

  private Integer intFromBytes(final ByteBuffer value, final ByteOrder byteOrder) {
    if (value == null) {
      return null;
    }

    BytesUtils.checkBytesSize(value, BYTES_LENGTH);
    value.rewind();
    return value.order(byteOrder).getInt();
  }
}
