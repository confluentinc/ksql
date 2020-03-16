package org.damian.ksql.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import java.util.Map;
import java.util.Collections;

/**
 * Class used to test UDFs. This is packaged in udf-example.jar
 */
@UdfDescription(name = "tomap", description = "converts things to a map")
public class ToMap {
  @Udf
  public Map<String, String> fromString(final String value) {
    return Collections.singletonMap(value, value);
  }
}
