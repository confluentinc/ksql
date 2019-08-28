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

package org.damian.ksql.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import java.math.BigDecimal;

/**
 * Class used to test the loading of UDFs. This is packaged in udf-failing-tests.jar
 * Attention: This test crashes the UdfLoader.
 */

@UdfDescription(
    name = "ReturnDecimalWithoutSchemaProvider",
    description = "A test-only UDF for testing 'SchemaProvider'")

public class ReturnDecimalWithoutSchemaProviderUdf {

  @Udf
  public BigDecimal foo(final BigDecimal p) {
    return new BigDecimal(1);
  }
}
