/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Objects;

public enum SerdeOption {

  /**
   * If the value schema contains only a single column, persist it wrapped within an envelope of
   * some kind, e.g. an Avro record, or JSON object.
   *
   * <p>If not set, any single column will be persisted using the default mechanism of the format.
   *
   * @see SerdeOption#UNWRAP_SINGLE_VALUES
   */
  WRAP_SINGLE_VALUES(SerdeFeature.WRAP_SINGLES),

  /**
   * If the value schema contains only a single field, persist it as an anonymous value.
   *
   * <p>If not set, any single field value schema will be persisted within an outer object, e.g.
   * JSON Object or Avro Record.
   *
   * @see SerdeOption#WRAP_SINGLE_VALUES
   */
  UNWRAP_SINGLE_VALUES(SerdeFeature.UNWRAP_SINGLES);

  private final SerdeFeature requiredFeature;

  SerdeOption(final SerdeFeature requiredFeature) {
    this.requiredFeature = Objects.requireNonNull(requiredFeature, "requiredFeature");
  }

  public SerdeFeature requiredFeature() {
    return requiredFeature;
  }

  @JsonCreator
  public static SerdeOption fromString(final String value) {
    return SerdeOption.valueOf(value);
  }
}
