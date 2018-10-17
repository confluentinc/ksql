package org.damian.ksql.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import java.util.List;
import java.util.Collections;

/**
 * Class used to test UDFs. This is packaged in udf-example.jar
 */
@UdfDescription(name = "tolist", description = "converts things to a list")
public class ToList {
  @Udf
  public List<String> fromString(final String value) {
    return Collections.singletonList(value);
  }
}
