package org.damian.ksql.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Class used to test UDFs. This is packaged in udf-example.jar
 */
@UdfDescription(name = "tostruct", description = "converts things to a struct")
public class ToStruct {
  @Udf(schema = "STRUCT<A VARCHAR>")
  public Struct fromString(final String value) {
    final Schema schema = SchemaBuilder.struct()
        .optional()
        .field("A", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    return new Struct(schema).put("A", value);
  }
}
