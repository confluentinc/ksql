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

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.BytesUtils;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.ByteBuffer;

@UdfDescription(
    name = "to_bytes",
    category = FunctionCategory.STRING,
    description = "Converts a STRING value in the specified encoding to BYTES. "
        + "The accepted encoders are 'hex', 'utf8', 'ascii', and 'base64'.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class ToBytes {
  @Udf(description = "Converts a STRING value in the specified encoding to BYTES. "
      + "The accepted encoders are 'hex', 'utf8', 'ascii', and 'base64'.")
  public ByteBuffer toBytes(
      @UdfParameter(description = "The string to convert.") final String value,
      @UdfParameter(description = "The type of encoding.") final String encoding) {
    return (value == null || encoding == null) ? null : ByteBuffer.wrap(BytesUtils.decode(value,
        BytesUtils.Encoding.from(encoding)));
  }
}
