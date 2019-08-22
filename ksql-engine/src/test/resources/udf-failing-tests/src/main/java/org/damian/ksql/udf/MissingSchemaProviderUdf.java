package org.damian.ksql.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
/**
 * Class used to test the loading of UDFs. This is packaged in udf-failing-tests.jar
 * Attention: This test crashes the UdfLoader.
 */

@UdfDescription(
    name = "MissingSchemaProviderMethod",
    description = "A test-only UDF for testing 'SchemaProvider'")

public class MissingSchemaProviderUdf {

  @Udf(schemaProvider = "provideSchema")
  public BigDecimal foo(@UdfParameter("justValue") final BigDecimal p) {
    return p;
  }
}
