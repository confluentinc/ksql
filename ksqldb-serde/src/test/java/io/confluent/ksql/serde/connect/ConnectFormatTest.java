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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeFeature;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConnectFormatTest {

  @Mock
  private Function<Schema, Schema> toKsqlTransformer;
  @Mock
  private ParsedSchema parsedSchema;
  @Mock
  private Schema connectSchema;
  @Mock
  private Schema transformedSchema;

  private TestFormat format;

  @Before
  public void setUp() {
    format = new TestFormat();

    when(toKsqlTransformer.apply(any())).thenReturn(transformedSchema);
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

  private final class TestFormat extends ConnectFormat {

    TestFormat() {
      super(toKsqlTransformer);
    }

    @Override
    protected Schema toConnectSchema(final ParsedSchema schema) {
      return connectSchema;
    }

    @Override
    protected ParsedSchema fromConnectSchema(final Schema schema, final FormatInfo formatInfo) {
      return parsedSchema;
    }

    @Override
    public String name() {
      return null;
    }

    @Override
    public Set<SerdeFeature> supportedFeatures() {
      return null;
    }

    @Override
    public KsqlSerdeFactory getSerdeFactory(final FormatInfo info) {
      return null;
    }
  }
}