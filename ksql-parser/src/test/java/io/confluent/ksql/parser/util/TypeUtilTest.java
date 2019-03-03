/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.parser.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.parser.tree.Array;
import io.confluent.ksql.parser.tree.Map;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.TypeUtil;
import java.util.Arrays;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TypeUtilTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldGetCorrectPrimitiveKsqlType() {
    final Type type0 = TypeUtil.getKsqlType(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    assertThat(type0.getSqlType(), equalTo(SqlType.BOOLEAN));

    final Type type1 = TypeUtil.getKsqlType(Schema.OPTIONAL_INT32_SCHEMA);
    assertThat(type1.getSqlType(), equalTo(SqlType.INTEGER));

    final Type type2 = TypeUtil.getKsqlType(Schema.OPTIONAL_FLOAT64_SCHEMA);
    assertThat(type2.getSqlType(), equalTo(SqlType.DOUBLE));

    final Type type3 = TypeUtil.getKsqlType(Schema.OPTIONAL_STRING_SCHEMA);
    assertThat(type3.getSqlType(), equalTo(SqlType.STRING));

    final Type type4 = TypeUtil.getKsqlType(Schema.OPTIONAL_INT64_SCHEMA);
    assertThat(type4.getSqlType(), equalTo(SqlType.BIGINT));
  }

  @Test
  public void shouldGetCorrectArrayKsqlType() {

    final Schema arraySchema = SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build();
    final Type type = TypeUtil.getKsqlType(arraySchema);
    assertThat(type.getSqlType(), equalTo(SqlType.ARRAY));
    assertThat(type, instanceOf(Array.class));
    assertThat(((Array) type).getItemType().getSqlType(), equalTo(SqlType.DOUBLE));
  }

  @Test
  public void shouldGetCorrectMapKsqlType() {

    final Schema mapSchema = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build();
    final Type type = TypeUtil.getKsqlType(mapSchema);
    assertThat(type.getSqlType(), equalTo(SqlType.MAP));
    assertThat(type, instanceOf(Map.class));
    assertThat(((Map) type).getValueType().getSqlType(), equalTo(SqlType.DOUBLE));
  }

  @Test
  public void shouldThrowOnNonStringKeyedMap() {
    // Given:
    final Schema mapSchema = SchemaBuilder
        .map(Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional()
        .build();

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unsupported map key type in schema: Schema{INT64}");

    // When:
    TypeUtil.getKsqlType(mapSchema);
  }

  @Test
  public void shouldGetCorrectStructKsqlType() {

    final Schema arraySchema = SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build();
    final Type type4 = TypeUtil.getKsqlType(arraySchema);
    assertThat(type4.getSqlType(), equalTo(SqlType.ARRAY));
    assertThat(type4, instanceOf(Array.class));
    assertThat(((Array) type4).getItemType().getSqlType(), equalTo(SqlType.DOUBLE));

    final Schema mapSchema = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build();
    final Type type5 = TypeUtil.getKsqlType(mapSchema);
    assertThat(type5.getSqlType(), equalTo(SqlType.MAP));
    assertThat(type5, instanceOf(Map.class));
    assertThat(((Map) type5).getValueType().getSqlType(), equalTo(SqlType.DOUBLE));

    final Schema structSchema1 = SchemaBuilder.struct()
        .field("COL1", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL2", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL3", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("COL4", arraySchema)
        .field("COL5", mapSchema)
        .optional().build();
    final Type type6 = TypeUtil.getKsqlType(structSchema1);
    assertThat(type6.getSqlType(), equalTo(SqlType.STRUCT));
    assertThat(type6, instanceOf(Struct.class));
    assertThat(((Struct) type6).getItems().get(0).getRight().getSqlType(), equalTo(SqlType.DOUBLE));
    assertThat(((Struct) type6).getItems().get(1).getRight().getSqlType(), equalTo(SqlType
                                                                                        .STRING));
    assertThat(((Struct) type6).getItems().get(2).getRight().getSqlType(), equalTo(SqlType
                                                                                        .BOOLEAN));
    assertThat(((Struct) type6).getItems().get(3).getRight().getSqlType(), equalTo(SqlType
                                                                                        .ARRAY));
    assertThat(((Struct) type6).getItems().get(4).getRight().getSqlType(), equalTo(SqlType
                                                                                        .MAP));

    final Schema structSchema2 = SchemaBuilder.struct()
        .field("COL1", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL2", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL3", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("COL4", arraySchema)
        .field("COL5", mapSchema)
        .field("COL6", structSchema1)
        .build();
    final Type type7 = TypeUtil.getKsqlType(structSchema2);
    assertThat(type6.getSqlType(), equalTo(SqlType.STRUCT));
    assertThat(type7, instanceOf(Struct.class));
    assertThat(((Struct) type7).getItems().get(0).getRight().getSqlType(), equalTo(SqlType.DOUBLE));
    assertThat(((Struct) type7).getItems().get(1).getRight().getSqlType(), equalTo(SqlType
                                                                                        .STRING));
    assertThat(((Struct) type7).getItems().get(2).getRight().getSqlType(), equalTo(SqlType
                                                                                        .BOOLEAN));
    assertThat(((Struct) type7).getItems().get(3).getRight().getSqlType(), equalTo(SqlType
                                                                                        .ARRAY));
    assertThat(((Struct) type7).getItems().get(4).getRight().getSqlType(), equalTo(SqlType
                                                                                        .MAP));
    assertThat(((Struct) type7).getItems().get(5).getRight().getSqlType(), equalTo(SqlType
                                                                                        .STRUCT));


  }

  @Test
  public void shouldGetCorrectPrimitiveSchema() {

    final Schema schema1 = TypeUtil.getTypeSchema(new PrimitiveType(SqlType.BIGINT));
    assertThat(schema1, equalTo(Schema.OPTIONAL_INT64_SCHEMA));

    final Schema schema2 = TypeUtil.getTypeSchema(new PrimitiveType(SqlType.BOOLEAN));
    assertThat(schema2, equalTo(Schema.OPTIONAL_BOOLEAN_SCHEMA));

    final Schema schema3 = TypeUtil.getTypeSchema(new PrimitiveType(SqlType.INTEGER));
    assertThat(schema3, equalTo(Schema.OPTIONAL_INT32_SCHEMA));

    final Schema schema4 = TypeUtil.getTypeSchema(new PrimitiveType(SqlType.DOUBLE));
    assertThat(schema4, equalTo(Schema.OPTIONAL_FLOAT64_SCHEMA));

    final Schema schema5 = TypeUtil.getTypeSchema(new PrimitiveType(SqlType.STRING));
    assertThat(schema5, equalTo(Schema.OPTIONAL_STRING_SCHEMA));

    final Schema schema6 = TypeUtil.getTypeSchema(new PrimitiveType(SqlType.BIGINT));
    assertThat(schema6, equalTo(Schema.OPTIONAL_INT64_SCHEMA));

  }

  @Test
  public void shouldGetCorrectArraySchema() {

    final Schema schema = TypeUtil.getTypeSchema(new Array(new PrimitiveType(SqlType.STRING)));
    assertThat(schema.type(), equalTo(Schema.Type.ARRAY));
    assertThat(schema.valueSchema().type(), equalTo(Schema.Type.STRING));
  }

  @Test
  public void shouldGetCorrectMapSchema() {

    final Schema schema = TypeUtil.getTypeSchema(new Map(new PrimitiveType(SqlType.DOUBLE)));
    assertThat(schema.type(), equalTo(Schema.Type.MAP));
    assertThat(schema.valueSchema().type(), equalTo(Schema.Type.FLOAT64));
  }

  @Test
  public void shouldGetCorrectSchema() {

    final Struct internalStruct = new Struct(Arrays.asList(
        new Pair<>("COL1", new PrimitiveType(SqlType.STRING)),
        new Pair<>("COL2", new PrimitiveType(SqlType.BIGINT)),
        new Pair<>("COL3", new PrimitiveType(SqlType.BOOLEAN))
    ));

    final Struct struct = new Struct(Arrays.asList(
        new Pair<>("COL1", new PrimitiveType(SqlType.STRING)),
        new Pair<>("COL2", new PrimitiveType(SqlType.BIGINT)),
        new Pair<>("COL3", new PrimitiveType(SqlType.BOOLEAN)),
        new Pair<>("COL4", new Array(new PrimitiveType(SqlType.STRING))),
        new Pair<>("COL5", new Map(new PrimitiveType(SqlType.DOUBLE))),
        new Pair<>("COL6", internalStruct)
    ));

    final Schema schema8 = TypeUtil.getTypeSchema(struct);
    assertThat(schema8.type(), equalTo(Schema.Type.STRUCT));
    assertThat(schema8.fields().size(), equalTo(6));
    assertThat(schema8.field("COL1").schema().type(), equalTo(Schema.Type.STRING));
    assertThat(schema8.field("COL4").schema().type(), equalTo(Schema.Type.ARRAY));
    assertThat(schema8.field("COL5").schema().type(), equalTo(Schema.Type.MAP));
    assertThat(schema8.field("COL6").schema().type(), equalTo(Schema.Type.STRUCT));
    final Schema netstedSchema = schema8.field("COL6").schema();
    assertThat(netstedSchema.fields().size(), equalTo(3));
    assertThat(netstedSchema.field("COL1").schema().type(), equalTo(Schema.Type.STRING));
    assertThat(netstedSchema.field("COL2").schema().type(), equalTo(Schema.Type.INT64));
    assertThat(netstedSchema.field("COL3").schema().type(), equalTo(Schema.Type.BOOLEAN));
  }
}
