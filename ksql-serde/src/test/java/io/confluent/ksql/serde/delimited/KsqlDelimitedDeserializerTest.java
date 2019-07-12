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

package io.confluent.ksql.serde.delimited;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlDelimitedDeserializerTest {

  private static final PersistenceSchema ORDER_SCHEMA = persistenceSchema(
      SchemaBuilder.struct()
      .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ORDERUNITS", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("COST", DecimalUtil.builder(4, 2).build())
      .build()
  );

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private KsqlDelimitedDeserializer deserializer;

  @Before
  public void before() {
    deserializer = new KsqlDelimitedDeserializer(ORDER_SCHEMA);
  }

  @Test
  public void shouldDeserializeDelimitedCorrectly() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,10.0,10.10\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    final Struct struct = deserializer.deserialize("", bytes);

    // Then:
    assertThat(struct.schema(), is(ORDER_SCHEMA.getConnectSchema()));
    assertThat(struct.get("ORDERTIME"), is(1511897796092L));
    assertThat(struct.get("ORDERID"), is(1L));
    assertThat(struct.get("ITEMID"), is("item_1"));
    assertThat(struct.get("ORDERUNITS"), is(10.0));
    assertThat(struct.get("COST"), is(new BigDecimal("10.10")));
  }

  @Test
  public void shouldDeserializeJsonCorrectlyWithEmptyFields() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,,\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    final Struct struct = deserializer.deserialize("", bytes);

    // Then:
    assertThat(struct.schema(), is(ORDER_SCHEMA.getConnectSchema()));
    assertThat(struct.get("ORDERTIME"), is(1511897796092L));
    assertThat(struct.get("ORDERID"), is(1L));
    assertThat(struct.get("ITEMID"), is("item_1"));
    assertThat(struct.get("ORDERUNITS"), is(nullValue()));
    assertThat(struct.get("COST"), is(nullValue()));
  }

  @Test
  public void shouldThrowIfRowHasTooFewColumns() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,\r\n".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException
        .expectCause(hasMessage(is("Unexpected field count, csvFields:4 schemaFields:5")));

    // When:
    deserializer.deserialize("", bytes);
  }

  @Test
  public void shouldThrowIfRowHasTooMayColumns() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,10.0,10.10,extra\r\n".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException
        .expectCause(hasMessage(is("Unexpected field count, csvFields:6 schemaFields:5")));

    // When:
    deserializer.deserialize("", bytes);
  }

  @Test
  public void shouldThrowIfTopLevelNotStruct() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("DELIMITED expects all top level schemas to be STRUCTs");

    // When:
    new KsqlDelimitedDeserializer(persistenceSchema(Schema.OPTIONAL_INT64_SCHEMA));
  }

  @Test
  public void shouldDeserializedTopLevelPrimitiveTypeIfSchemaHasOnlySingleField() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        SchemaBuilder.struct()
        .field("id", Schema.OPTIONAL_INT32_SCHEMA)
        .build()
    );

    final KsqlDelimitedDeserializer deserializer = new KsqlDelimitedDeserializer(schema);

    final byte[] bytes = "10".getBytes(StandardCharsets.UTF_8);

    // When:
    final Struct result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result.get("id"), CoreMatchers.is(10));
  }

  @Test
  public void shouldDeserializeDecimal() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        SchemaBuilder.struct()
          .field("cost", DecimalUtil.builder(4, 2))
          .build()
    );
    final KsqlDelimitedDeserializer deserializer = new KsqlDelimitedDeserializer(schema);

    final byte[] bytes = "01.12".getBytes(StandardCharsets.UTF_8);

    // When:
    final Struct result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result.get("cost"), is(new BigDecimal("01.12")));
  }

  @Test
  public void shouldDeserializeDecimalWithoutLeadingZeros() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        SchemaBuilder.struct()
            .field("cost", DecimalUtil.builder(4, 2))
            .build()
    );
    final KsqlDelimitedDeserializer deserializer = new KsqlDelimitedDeserializer(schema);

    final byte[] bytes = "1.12".getBytes(StandardCharsets.UTF_8);

    // When:
    final Struct result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result.get("cost"), is(new BigDecimal("01.12")));
  }

  @Test
  public void shouldThrowOnDeserializedTopLevelPrimitiveWhenSchemaHasMoreThanOneField() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        SchemaBuilder.struct()
        .field("id", Schema.OPTIONAL_INT32_SCHEMA)
        .field("id2", Schema.OPTIONAL_INT32_SCHEMA)
        .build()
    );

    final KsqlDelimitedDeserializer deserializer = new KsqlDelimitedDeserializer(schema);

    final byte[] bytes = "10".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(instanceOf(KsqlException.class));
    expectedException.expectCause(
        hasMessage(CoreMatchers.is("Unexpected field count, csvFields:1 schemaFields:2")));

    // When:
    deserializer.deserialize("", bytes);
  }

  @Test
  public void shouldThrowOnArrayTypes() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        SchemaBuilder.struct()
        .field("ids", SchemaBuilder
            .array(Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
        .build()
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("DELIMITED does not support type: ARRAY, field: ids");

    // When:
    new KsqlDelimitedDeserializer(schema);
  }

  @Test
  public void shouldThrowOnMapTypes() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        SchemaBuilder.struct()
        .field("ids", SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build())
        .build()
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("DELIMITED does not support type: MAP, field: ids");

    // When:
    new KsqlDelimitedDeserializer(schema);
  }

  @Test
  public void shouldThrowOnStructTypes() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        SchemaBuilder.struct()
        .field("ids", SchemaBuilder
            .struct()
            .field("f0", Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
        .build()
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("DELIMITED does not support type: STRUCT, field: ids");

    // When:
    new KsqlDelimitedDeserializer(schema);
  }

  private static PersistenceSchema persistenceSchema(final Schema connectSchema) {
    return PersistenceSchema.of((ConnectSchema) connectSchema);
  }
}
