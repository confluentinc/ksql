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

package io.confluent.ksql.serde;

import org.apache.kafka.common.errors.SerializationException;

public class KsqlSerializationException extends SerializationException {

  private static final long serialVersionUID = 1L;
  private final String topic;

  public KsqlSerializationException(
      final String topic, final String message, final Throwable throwable) {
    super(message, throwable);
    this.topic = topic;
  }

  public KsqlSerializationException(final String topic, final String message) {
    super(message);
    this.topic = topic;
  }

  public String getTopic() {
    return topic;
  }

}
