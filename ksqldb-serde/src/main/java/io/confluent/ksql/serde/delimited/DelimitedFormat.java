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

package io.confluent.ksql.serde.delimited;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.serde.Delimiter;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeFeature;
import java.util.Optional;
import java.util.Set;

public final class DelimitedFormat implements Format {

  private static final Set<SerdeFeature> SUPPORTED_FEATURES = ImmutableSet.of(
  );

  public static final String DELIMITER = "delimiter";
  public static final String NAME = "DELIMITED";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Set<SerdeFeature> supportedFeatures() {
    return SUPPORTED_FEATURES;
  }

  @Override
  public Set<String> getSupportedProperties() {
    return ImmutableSet.of(DELIMITER);
  }

  @Override
  public KsqlSerdeFactory getSerdeFactory(final FormatInfo info) {
    return new KsqlDelimitedSerdeFactory(
        Optional
            .ofNullable(info.getProperties().get(DELIMITER))
            .map(Delimiter::parse)
    );
  }
}
