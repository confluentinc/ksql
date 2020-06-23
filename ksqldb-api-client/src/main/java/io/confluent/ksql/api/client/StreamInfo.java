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
   * Returns the name of this stream.
   *
   * @return stream name
   */
  String getName();

  /**
   * Returns the name of the Kafka topic underlying this ksqlDB stream.
   *
   * @return topic name
   */
  String getTopic();

  /**
   * Returns the format of the data in this stream.
   *
   * @return the format
   */
  String getFormat();

}