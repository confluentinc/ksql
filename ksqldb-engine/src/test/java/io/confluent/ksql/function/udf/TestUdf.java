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

package io.confluent.ksql.function.udf;

import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdfDescription(name="test_udf", description = "test")
@SuppressWarnings("unused")
public class TestUdf {

  private static final SqlStruct RETURN =
      SqlStruct.builder().field("A", SqlTypes.STRING).build();

  @Udf(description = "returns the method name")
  public String doStuffIntString(final int arg1, final String arg2) {
    return "doStuffIntString";
  }

  @Udf(description = "returns the method name")
  public String doStuffLongString(final long arg1, final String arg2) {
    return "doStuffLongString";
  }

  @Udf(description = "returns the method name")
  public String doStuffLongLongString(final long arg1, final long arg2, final String arg3) {
    return "doStuffLongLongString";
  }

  @Udf(description = "returns method name")
  public String doStuffLongVarargs(final long... longs) {
    return "doStuffLongVarargs";
  }

  @Udf(description = "returns the value of 'A'")
  public String doStuffStruct(
      @UdfParameter(schema = "STRUCT<A VARCHAR>") final Struct struct
  ) {
    return struct.getString("A");
  }

  @Udf(description = "returns the value of 'STRUCT<A VARCHAR>'", schemaProvider = "structProvider")
  public Struct returnStructStuff() {
    return new Struct(
        SchemaBuilder.struct().field("A", SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build()
    ).put("A", "foo");
  }

  @UdfSchemaProvider
  public SqlType structProvider(final List<SqlType> params) {
    return RETURN;
  }
}
