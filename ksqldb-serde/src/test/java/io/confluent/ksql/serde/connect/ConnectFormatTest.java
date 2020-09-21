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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.EnabledSerdeFeatures;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.unwrapped.UnwrappedDeserializer;
import io.confluent.ksql.serde.unwrapped.UnwrappedSerializer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"unchecked", "rawtypes"})
@RunWith(MockitoJUnitRunner.class)
public class ConnectFormatTest {

  private static final ConnectSchema SINGLE_FIELD_SCHEMA = (ConnectSchema) SchemaBuilder.struct()
      .field("bob", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  @Mock
  private Function<Schema, Schema> toKsqlTransformer;
  @Mock
  private ParsedSchema parsedSchema;
  @Mock
  private Schema connectSchema;
  @Mock
  private Schema transformedSchema;
  @Mock
  private PersistenceSchema persistenceSchema;
  @Mock
  private FormatInfo formatInfo;
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
  private Schema capturedConnectSchema;
  private Set<SerdeFeature> supportedFeatures;

  @Before
  public void setUp() {
    capturedConnectSchema = null;
    supportedFeatures = EnumSet.allOf(SerdeFeature.class);

    format = spy(new TestFormat());

    when(toKsqlTransformer.apply(any())).thenReturn(transformedSchema);
    when(connectSchema.type()).thenReturn(Type.STRUCT);

    when(serde.serializer()).thenReturn(serializer);
    when(serde.deserializer()).thenReturn(deserializer);

    when(persistenceSchema.features()).thenReturn(EnabledSerdeFeatures.of());
  }

  @Test
  public void shouldSupportSchemaInference() {
    assertThat(format.supportsSchemaInference(), is(true));
  }

  @Test
  public void shouldPassConnectSchemaReturnedBySubclassToTranslator() {
    // When:
    format.toColumns(parsedSchema);

    // Then:
    verify(toKsqlTransformer).apply(connectSchema);
  }

  @Test
  public void shouldConvertTransformedConnectSchemaToColumns() {
    // Given:
    when(transformedSchema.fields()).thenReturn(ImmutableList.of(
        new Field("bob", 0, Schema.OPTIONAL_INT32_SCHEMA),
        new Field("bert", 2, Schema.OPTIONAL_INT64_SCHEMA)
    ));

    // When:
    final List<SimpleColumn> result = format.toColumns(parsedSchema);

    // Then:
    assertThat(result, hasSize(2));
    assertThat(result.get(0).name(), is(ColumnName.of("bob")));
    assertThat(result.get(0).type(), is(SqlTypes.INTEGER));
    assertThat(result.get(1).name(), is(ColumnName.of("bert")));
    assertThat(result.get(1).type(), is(SqlTypes.BIGINT));
  }

  @Test
  public void shouldThrowOnUnwrappedSchemaIfNotSupportedWhenBuildingColumns() {
    // Given:
    supportedFeatures = ImmutableSet.of();

    when(connectSchema.type()).thenReturn(Type.INT32);

    // When:
    final Exception e = assertThrows(KsqlException.class,
        () -> format.toColumns(parsedSchema)
    );

    // Then:
    assertThat(e.getMessage(), is("Schema returned from schema registry is anonymous type, "
        + "but format TestFormat does not support anonymous types. "
        + "schema: parsedSchema"));
  }

  @Test
  public void shouldThrowOnUnsupportedFeatureWhenBuildingParsedSchema() {
    // Given:
    supportedFeatures = ImmutableSet.of();

    // When:
    final Exception e = assertThrows(IllegalArgumentException.class,
        () -> format.toParsedSchema(
            PersistenceSchema.from(
                ImmutableList.of(createColumn("vic", SqlTypes.STRING)),
                EnabledSerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)
            ),
            formatInfo
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unsupported feature"));
  }

  @Test
  public void shouldSupportBuildingColumnsFromPrimitiveSchema() {
    // Given:
    when(connectSchema.type()).thenReturn(Type.INT32);

    // When:
    format.toColumns(parsedSchema);

    // Then:
    verify(toKsqlTransformer).apply(SchemaBuilder.struct()
        .field("ROWVAL", connectSchema)
        .build());
  }

  @Test
  public void shouldSupportBuildingPrimitiveSchemas() {
    // When:
    format.toParsedSchema(
        PersistenceSchema.from(
            ImmutableList.of(createColumn("bob", SqlTypes.INTEGER)),
            EnabledSerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)
        ),
        formatInfo
    );

    // Then:
    assertThat(capturedConnectSchema, is(SchemaBuilder.int32().optional().build()));
  }

  @Test
  public void shouldCallSubclassToCreateOuterWhenWrapped() {
    // Given:
    final SimpleColumn singleColumn = createColumn("bob", SqlTypes.INTEGER);
    when(persistenceSchema.columns()).thenReturn(ImmutableList.of(singleColumn));

    // When:
    format.getSerde(persistenceSchema, formatProps, config, srFactory);

    // Then:
    verify(format)
        .getConnectSerde(SINGLE_FIELD_SCHEMA, formatProps, config, srFactory, Struct.class);
  }

  @Test
  public void shouldCallSubclassToCreateInnerWhenUnwrapped() {
    // Given:
    final SimpleColumn singleColumn = createColumn("bob", SqlTypes.INTEGER);
    when(persistenceSchema.columns()).thenReturn(ImmutableList.of(singleColumn));
    when(persistenceSchema.features())
        .thenReturn(EnabledSerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    final ConnectSchema fieldSchema = (ConnectSchema) SINGLE_FIELD_SCHEMA.fields().get(0).schema();

    // When:
    final Serde<List<?>> result = format
        .getSerde(persistenceSchema, formatProps, config, srFactory);

    // Then:
    verify(format)
        .getConnectSerde(fieldSchema, formatProps, config, srFactory, Integer.class);

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
        .getSerde(persistenceSchema, formatProps, config, srFactory)
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

    TestFormat() {
      super(toKsqlTransformer);
    }

    @Override
    protected Schema toConnectSchema(final ParsedSchema schema) {
      return connectSchema;
    }

    @Override
    protected ParsedSchema fromConnectSchema(
        final Schema schema,
        final FormatInfo formatInfo
    ) {
      capturedConnectSchema = schema;
      return parsedSchema;
    }

    @Override
    public String name() {
      return "TestFormat";
    }

    @Override
    public Set<SerdeFeature> supportedFeatures() {
      return supportedFeatures;
    }

    @Override
    protected <T> Serde<T> getConnectSerde(
        final ConnectSchema connectSchema,
        final Map<String, String> formatProps,
        final KsqlConfig config,
        final Supplier<SchemaRegistryClient> srFactory,
        final Class<T> targetType
    ) {
      capturedConnectSchema = connectSchema;
      return serde;
    }
  }
}