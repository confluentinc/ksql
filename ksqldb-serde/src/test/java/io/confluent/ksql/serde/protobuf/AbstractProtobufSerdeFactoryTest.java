/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.serde.protobuf;

import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.serde.SerdeFactory;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public abstract class AbstractProtobufSerdeFactoryTest {
  @Mock
  private KsqlConfig config;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;

  abstract SerdeFactory getSerdeFactory();

  @Before
  public void setUp() throws Exception {
    when(config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)).thenReturn("http://localhost:8088");
  }

  @Test
  public void shouldNotThrowOnDecimal() {
    // Given:
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder.struct()
        .field("f0", SchemaBuilder.array(DecimalUtil.builder(10, 2)))
        .build();

    // When:
    getSerdeFactory().createSerde(schema, config, srClientFactory, Struct.class, false);

    // Then (did not throw)
  }

  @Test
  public void shouldNotThrowOnNonDecimal() {
    // Given:
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder.struct()
        .field("f0", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA))
        .build();

    // When:
    getSerdeFactory().createSerde(schema, config, srClientFactory,
        Struct.class, false);

    // Then (did not throw)
  }
}
