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

package io.confluent.ksql.serde.json;

import com.google.common.collect.ImmutableSet;
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.connect.ConnectFormat;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;

public class JsonFormat extends ConnectFormat {

  private static final Set<SerdeFeature> SUPPORTED_FEATURES = ImmutableSet.of(
      SerdeFeature.WRAP_SINGLES,
      SerdeFeature.UNWRAP_SINGLES
  );

  @Override
  public Set<SerdeFeature> supportedFeatures() {
    return SUPPORTED_FEATURES;
  }

  public static final String NAME = JsonSchema.TYPE;

  private final JsonSchemaData jsonData = new JsonSchemaData();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean supportsSchemaInference() {
    return false;
  }

  @Override
  public KsqlSerdeFactory getSerdeFactory(final FormatInfo info) {
    return new KsqlJsonSerdeFactory(false);
  }

  @Override
  protected Schema toConnectSchema(final ParsedSchema schema) {
    return jsonData.toConnectSchema((JsonSchema) schema);
  }

  @Override
  protected ParsedSchema fromConnectSchema(final Schema schema, final FormatInfo formatInfo) {
    return jsonData.fromConnectSchema(schema);
  }
}
