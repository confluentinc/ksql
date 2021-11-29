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
import io.confluent.ksql.util.KsqlException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@UdfDescription(
    name = "bigint_from_bytes",
    category = FunctionCategory.CONVERSIONS,
    description = "Converts a BYTES value to a BIGINT type.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class BigIntFromBytes {
  private static final int BYTES_LENGTH = 8;

  @Udf(description = "Converts a BYTES value to a BIGINT type.")
  public Long bigIntFromBytes(
      @UdfParameter(description = "The bytes value to convert.")
      final ByteBuffer value
  ) {
    return bigIntFromBytes(value, ByteOrder.BIG_ENDIAN);
  }

  @Udf(description = "Converts a BYTES value to a BIGINT type.")
  public Long bigIntFromBytes(
      @UdfParameter(description = "The bytes value to convert.")
      final ByteBuffer value,
      @UdfParameter(description = "The byte order of the number bytes representation")
      final String byteOrder
  ) {
    if (byteOrder.equalsIgnoreCase(ByteOrder.BIG_ENDIAN.toString())) {
      return bigIntFromBytes(value, ByteOrder.BIG_ENDIAN);
    } else if (byteOrder.equalsIgnoreCase(ByteOrder.LITTLE_ENDIAN.toString())) {
      return bigIntFromBytes(value, ByteOrder.LITTLE_ENDIAN);
    } else {
      throw new KsqlException(String.format(
          "Byte order must be BIG_ENDIAN or LITTLE_ENDIAN. Unknown byte order '%s'.", byteOrder));
    }
  }

  private Long bigIntFromBytes(final ByteBuffer value, final ByteOrder byteOrder) {
    if (value == null) {
      return null;
    }

    BytesUtils.checkBytesSize(value, BYTES_LENGTH);
    value.rewind();
    return value.order(byteOrder).getLong();
  }
}
