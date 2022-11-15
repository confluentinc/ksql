/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.serde.none;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.voids.KsqlVoidSerde;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NoneFormatTest {

  @Mock
  private PersistenceSchema schema;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private SimpleColumn column;
  private Map<String, String> formatProps = new HashMap<>();
  private NoneFormat format;

  @Before
  public void setUp() {
    format = new NoneFormat();

    when(schema.columns()).thenReturn(ImmutableList.of());
    when(schema.features()).thenReturn(SerdeFeatures.of());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnUnsupportedFeatures() {
    // Given:
    when(schema.features()).thenReturn(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // When:
    format.getSerde(schema, formatProps, ksqlConfig, srClientFactory, false);
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnUnsupportedProps() {
    // Given:
    formatProps = ImmutableMap.of("some", "prop");

    // When:
    format.getSerde(schema, formatProps, ksqlConfig, srClientFactory, false);
  }

  @Test
  public void shouldThrowOnColumns() {
    // Given:
    when(schema.columns()).thenReturn(ImmutableList.of(column));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> format.getSerde(schema, formatProps, ksqlConfig, srClientFactory, false)
    );

    // Then:
    assertThat(e.getMessage(),
        is("The 'NONE' format can only be used when no columns are defined. Got: [column]"));
  }

  @Test
  public void shouldReturnVoidSerde() {
    // When:
    final Serde<List<?>> serde = format.getSerde(schema, formatProps, ksqlConfig, srClientFactory, false);

    // Then:
    assertThat(serde, instanceOf(KsqlVoidSerde.class));
  }
}