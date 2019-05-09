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

package io.confluent.ksql.util;

/**
 * An error that represents a user access issue with a Kafka topic. This exception is used
 * by the KSQL REST resources to differentiate between a user error and an access problem
 * with a topic.
 */
public class KsqlTopicAccessException extends KsqlException {
  private static final String TOPIC_ACCESS_ERROR_MESSAGE = "Topic '%s' does not exist, or "
      + "the KSQL user does not have access to the topic.";

  public KsqlTopicAccessException(final String topicName) {
    super(String.format(TOPIC_ACCESS_ERROR_MESSAGE, topicName));
  }

  public KsqlTopicAccessException(final Throwable cause) {
    super(cause);
  }
}