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

package io.confluent.ksql.serde.avro;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.connect.ConnectFormat;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;

public final class AvroFormat extends ConnectFormat {

  public static final String FULL_SCHEMA_NAME = "fullSchemaName";
  public static final String NAME = AvroSchema.TYPE;

  private final AvroData avroData = new AvroData(new AvroDataConfig(ImmutableMap.of()));

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Set<String> getSupportedProperties() {
    return ImmutableSet.of(FULL_SCHEMA_NAME);
  }

  @Override
  public Set<String> getInheritableProperties() {
    return ImmutableSet.of();
  }

  @Override
  public KsqlSerdeFactory getSerdeFactory(final FormatInfo info) {
    final String schemaFullName = info
        .getProperties()
        .getOrDefault(FULL_SCHEMA_NAME, KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME);

    return new KsqlAvroSerdeFactory(schemaFullName);
  }

  protected Schema toConnectSchema(final ParsedSchema schema) {
    return avroData.toConnectSchema(((AvroSchema) schema).rawSchema());
  }

  protected ParsedSchema fromConnectSchema(final Schema schema) {
    return new AvroSchema(avroData.fromConnectSchema(schema));
  }
}
