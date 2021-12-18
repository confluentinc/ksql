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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.avro.AvroProperties;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConnectFormatSchemaTranslatorTest {

  @Mock
  private ConnectKsqlSchemaTranslator connectKsqlTranslator;
  @Mock
  private ParsedSchema parsedSchema;
  @Mock
  private Schema connectSchema;
  @Mock
  private Schema transformedSchema;
  @Mock
  private Map<String, String> formatProps;
  @Mock
  private ConnectFormat format;
  @Mock
  private ConnectSchemaTranslator innerTranslator;

  private ConnectFormatSchemaTranslator translator;

  @Before
  public void setUp() {
    when(connectKsqlTranslator.toKsqlSchema(any())).thenReturn(transformedSchema);
    when(connectSchema.type()).thenReturn(Type.STRUCT);

    when(format.getConnectSchemaTranslator(any())).thenReturn(innerTranslator);

    translator = new ConnectFormatSchemaTranslator(format, formatProps, connectKsqlTranslator);

    when(innerTranslator.toConnectSchema(parsedSchema)).thenReturn(connectSchema);
  }

  @Test
  public void shouldGetTranslatorWithCorrectProps() {
    verify(format).getConnectSchemaTranslator(formatProps);
  }

  @Test
  public void shouldPassConnectSchemaReturnedBySubclassToTranslator() {
    // When:
    translator.toColumns(parsedSchema, SerdeFeatures.of(), false);

    // Then:
    verify(connectKsqlTranslator).toKsqlSchema(connectSchema);
  }

  @Test
  public void shouldConvertTransformedConnectSchemaToColumns() {
    // Given:
    when(transformedSchema.fields()).thenReturn(ImmutableList.of(
        new Field("bob", 0, Schema.OPTIONAL_INT32_SCHEMA),
        new Field("bert", 2, Schema.OPTIONAL_INT64_SCHEMA)
    ));

    // When:
    final List<SimpleColumn> result = translator.toColumns(parsedSchema, SerdeFeatures.of(), false);

    // Then:
    assertThat(result, hasSize(2));
    assertThat(result.get(0).name(), is(ColumnName.of("bob")));
    assertThat(result.get(0).type(), is(SqlTypes.INTEGER));
    assertThat(result.get(1).name(), is(ColumnName.of("bert")));
    assertThat(result.get(1).type(), is(SqlTypes.BIGINT));
  }

  @Test
  public void shouldThrowOnUnwrappedSchemaIfUnwrapSingleIsFalse() {
    // Given:
    when(connectSchema.type()).thenReturn(Type.INT32);
    when(format.supportedFeatures()).thenReturn(Collections.singleton(SerdeFeature.UNWRAP_SINGLES));

    // When:
    final Exception e = assertThrows(KsqlException.class,
        () -> translator.toColumns(parsedSchema, SerdeFeatures.of(), false)
    );

    // Then:
    assertThat(e.getMessage(), is("Schema returned from schema registry is anonymous type. "
        + "To use this schema with ksqlDB, set 'WRAP_SINGLE_VALUE=false' in the "
        + "WITH clause properties."));
  }

  @Test
  public void shouldThrowOnUnsupportedFeatureWhenBuildingParsedSchema() {
    // When:
    final Exception e = assertThrows(IllegalArgumentException.class,
        () -> translator.toParsedSchema(
            PersistenceSchema.from(
                ImmutableList.of(createColumn("vic", SqlTypes.STRING)),
                SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)
            )
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unsupported feature"));
  }

  @Test
  public void shouldSupportBuildingColumnsFromPrimitiveValueSchema() {
    // Given:
    when(format.supportedFeatures()).thenReturn(Collections.singleton(SerdeFeature.UNWRAP_SINGLES));

    // When:
    translator.toColumns(parsedSchema, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES), false);

    // Then:
    verify(connectKsqlTranslator).toKsqlSchema(SchemaBuilder.struct()
        .field("ROWVAL", connectSchema)
        .build());
  }

  @Test
  public void shouldSupportBuildingColumnsFromPrimitiveKeySchema() {
    // Given:
    when(format.supportedFeatures()).thenReturn(Collections.singleton(SerdeFeature.UNWRAP_SINGLES));

    // When:
    translator.toColumns(parsedSchema, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES), true);

    // Then:
    verify(connectKsqlTranslator).toKsqlSchema(SchemaBuilder.struct()
        .field("ROWKEY", connectSchema)
        .build());
  }

  @Test
  public void shouldSupportBuildingPrimitiveSchemas() {
    // Given:
    when(format.supportedFeatures()).thenReturn(ImmutableSet.of(SerdeFeature.UNWRAP_SINGLES));


    // When:
    translator.toParsedSchema(
        PersistenceSchema.from(
            ImmutableList.of(createColumn("bob", SqlTypes.INTEGER)),
            SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)
        )
    );

    // Then:
    verify(innerTranslator).fromConnectSchema(SchemaBuilder.int32().optional().build());
  }

  private static SimpleColumn createColumn(final String name, final SqlType sqlType) {
    final SimpleColumn column = mock(SimpleColumn.class);
    when(column.name()).thenReturn(ColumnName.of(name));
    when(column.type()).thenReturn(sqlType);
    return column;
  }
}