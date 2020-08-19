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

package io.confluent.ksql.serde;


/**
 * Optional features a serde may support
 */
public enum SerdeFeature {

  /**
   * If the data being serialized contains only a single column, persist it wrapped within an
   * envelope of some kind, e.g. an Avro record, or JSON object.
   *
   * <p>If not set, any single column will be persisted using the default mechanism of the format.
   *
   * @see SerdeFeature#UNWRAP_SINGLES
   */
  WRAP_SINGLES,

  /**
   * If the key/value being serialized contains only a single column, persist it as an anonymous
   * value.
   *
   * <p>If not set, any single column will be persisted using the default mechanism of the format.
   *
   * @see SerdeFeature#WRAP_SINGLES
   */
  UNWRAP_SINGLES;
}
