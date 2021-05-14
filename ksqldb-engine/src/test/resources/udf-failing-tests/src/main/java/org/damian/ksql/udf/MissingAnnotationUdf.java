/*
 * Copyright 2021 Confluent Inc.
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
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

/**
 * Class used to test the loading of UDFs. This is packaged in udf-failing-tests.jar
 * Attention: This test crashes the UdfLoader.
 */

@UdfDescription(
    name = "MissingAnnotation",
    description = "A test-only UDF for testing 'SchemaProvider'")

public class MissingAnnotationUdf {

  @Udf
  public BigDecimal foo(@UdfParameter("justValue") final BigDecimal p) {
    return p;
  }

  @UdfSchemaProvider
  public SqlType provideSchema(List<SqlType> params) {
    return SqlDecimal.of(2, 1);
  }
}
