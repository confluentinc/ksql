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

package io.confluent.ksql.test.model;

import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import java.util.Optional;
import org.junit.Test;

public class SchemaNodeTest {

  private static final SchemaNode INSTANCE = new SchemaNode(
    "Some Logical Schema",
      Optional.of(KeyFormat.nonWindowed(FormatInfo.of("Avro"), SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES))),
      Optional.of(ValueFormat.of(FormatInfo.of("Protobuf"), SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)))
  );

  private static final SchemaNode NO_KEY_FORMAT = new SchemaNode(
      "Some Logical Schema",
      Optional.empty(),
      Optional.of(ValueFormat.of(FormatInfo.of("Protobuf"), SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)))
  );

  private static final SchemaNode NO_VALUE_FORMAT = new SchemaNode(
      "Some Logical Schema",
      Optional.of(KeyFormat.nonWindowed(FormatInfo.of("Avro"), SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES))),
      Optional.empty()
  );

  @Test
  public void shouldRoundTrip() {
    ModelTester.assertRoundTrip(INSTANCE);
  }

  @Test
  public void shouldRoundTripNoKeyFormat() {
    ModelTester.assertRoundTrip(NO_KEY_FORMAT);
  }

  @Test
  public void shouldRoundTripNoValueFormat() {
    ModelTester.assertRoundTrip(NO_VALUE_FORMAT);
  }
}