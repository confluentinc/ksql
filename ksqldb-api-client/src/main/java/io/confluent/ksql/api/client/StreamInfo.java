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

package io.confluent.ksql.api.client;

/**
 * Metadata for a ksqlDB stream.
 */
public interface StreamInfo {

  /**
   * @return the name of this stream
   */
  String getName();

  /**
   * @return the name of the Kafka topic underlying this ksqlDB stream
   */
  String getTopic();

  /**
   * @deprecated since 0.14, use {@link #getValueFormat()}
   */
  @Deprecated
  default String getFormat() {
    return getValueFormat();
  }

  /**
   * @return the key format of the data in this stream
   */
  String getKeyFormat();

  /**
   * @return the value format of the data in this stream
   */
  String getValueFormat();

  /**
   * @return whether the key is windowed.
   */
  boolean isWindowed();
}