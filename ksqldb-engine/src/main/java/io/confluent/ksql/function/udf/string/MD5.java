/*
 * Copyright 2023 Confluent Inc.
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
import org.apache.commons.codec.digest.DigestUtils;

@UdfDescription(
    name = "md5",
    category = FunctionCategory.STRING,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Computes the hex-encoded md5 hash of the given string"
)
public class MD5 {

  @Udf(description = "Returns the hex-encoded md5 hash of the input string")
  public String md5(
      @UdfParameter(description = "The input string") final String s
  ) {
    if (s == null) {
      return null;
    }
    return DigestUtils.md5Hex(s);
  }
}
