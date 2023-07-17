/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.serde.connect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.unwrapped.UnwrappedDeserializer;
import io.confluent.ksql.serde.unwrapped.UnwrappedSerializer;
import io.confluent.ksql.util.KsqlConfig;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
@SuppressWarnings({"unchecked", "rawtypes", "ResultOfMethodCallIgnored"})
@RunWith(MockitoJUnitRunner.class)
public class ConnectFormatTest {

  private static final ConnectSchema SINGLE_FIELD_SCHEMA = (ConnectSchema) SchemaBuilder.struct()
      .field("bob", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  @Mock
  private PersistenceSchema persistenceSchema;
  @Mock
  private Serde serde;
  @Mock
  private Map<String, String> formatProps;
  @Mock
  private KsqlConfig config;
  @Mock
  private Supplier<SchemaRegistryClient> srFactory;
  @Mock
  private Serializer serializer;
  @Mock
  private Deserializer deserializer;

  private TestFormat format;
  private Set<SerdeFeature> supportedFeatures;

  @Before
  public void setUp() {
    supportedFeatures = EnumSet.allOf(SerdeFeature.class);

    format = spy(new TestFormat());

    when(serde.serializer()).thenReturn(serializer);
    when(serde.deserializer()).thenReturn(deserializer);

    when(persistenceSchema.features()).thenReturn(SerdeFeatures.of());
  }

  @Test
  public void shouldCallSubclassToCreateOuterWhenWrapped() {
    // Given:
    final SimpleColumn singleColumn = createColumn("bob", SqlTypes.INTEGER);
    when(persistenceSchema.columns()).thenReturn(ImmutableList.of(singleColumn));

    // When:
    format.getSerde(persistenceSchema, formatProps, config, srFactory, false);

    // Then:
    verify(format)
        .getConnectSerde(SINGLE_FIELD_SCHEMA, formatProps, config, srFactory, Struct.class, false);
  }

  @Test
  public void shouldCallSubclassToCreateInnerWhenUnwrapped() {
    // Given:
    final SimpleColumn singleColumn = createColumn("bob", SqlTypes.INTEGER);
    when(persistenceSchema.columns()).thenReturn(ImmutableList.of(singleColumn));
    when(persistenceSchema.features())
        .thenReturn(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    final ConnectSchema fieldSchema = (ConnectSchema) SINGLE_FIELD_SCHEMA.fields().get(0).schema();

    // When:
    final Serde<List<?>> result = format
        .getSerde(persistenceSchema, formatProps, config, srFactory, false);

    // Then:
    verify(format)
        .getConnectSerde(fieldSchema, formatProps, config, srFactory, Integer.class, false);

    assertThat(result.serializer(), instanceOf(UnwrappedSerializer.class));
    assertThat(result.deserializer(), instanceOf(UnwrappedDeserializer.class));
  }

  @Test
  public void shouldThrowOnSerializationIfStructColumnValueDoesNotMatchSchema() {
    // Given:
    final SimpleColumn singleColumn = createColumn(
        "bob",
        SqlTypes.struct()
            .field("vic", SqlTypes.STRING)
            .build()
    );
    when(persistenceSchema.columns()).thenReturn(ImmutableList.of(singleColumn));

    final ConnectSchema connectSchema = (ConnectSchema) SchemaBuilder.struct()
        .field("vic", Schema.STRING_SCHEMA)
        .build();

    final Serializer<List<?>> serializer = format
        .getSerde(persistenceSchema, formatProps, config, srFactory, false)
        .serializer();

    final List<?> values = ImmutableList.of(new Struct(connectSchema));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize("topicName", values)
    );

    // Then:
    assertThat(e.getMessage(), is(
        "Failed to prepare Struct value field 'bob' for serialization. "
            + "This could happen if the value was produced by a user-defined function "
            + "where the schema has non-optional return types. ksqlDB requires all "
            + "schemas to be optional at all levels of the Struct: the Struct itself, "
            + "schemas for all fields within the Struct, and so on."
    ));
  }

  private static SimpleColumn createColumn(final String name, final SqlType sqlType) {
    final SimpleColumn column = mock(SimpleColumn.class);
    when(column.name()).thenReturn(ColumnName.of(name));
    when(column.type()).thenReturn(sqlType);
    return column;
  }

  private final class TestFormat extends ConnectFormat {

    @Override
    public String name() {
      return "TestFormat";
    }

    @Override
    public Set<SerdeFeature> supportedFeatures() {
      return supportedFeatures;
    }

    @Override
    protected ConnectSchemaTranslator getConnectSchemaTranslator(
        final Map<String, String> formatProps
    ) {
      return null;
    }

    @Override
    protected <T> Serde<T> getConnectSerde(
        final ConnectSchema connectSchema,
        final Map<String, String> formatProps,
        final KsqlConfig config,
        final Supplier<SchemaRegistryClient> srFactory,
        final Class<T> targetType,
        final boolean isKey
    ) {
      return serde;
    }
  }
}