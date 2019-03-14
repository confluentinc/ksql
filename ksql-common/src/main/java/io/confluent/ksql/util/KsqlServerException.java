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

import org.apache.kafka.streams.errors.StreamsException;

/**
 * A typed error which represents an internal server issue as opposed to
 * {@link KsqlException}, which represents a user error.
 */
public class KsqlServerException extends StreamsException {

  public KsqlServerException(final String message) {
    super(message);
  }

  public KsqlServerException(final String message, final Throwable throwable) {
    super(message, throwable);
  }

  public KsqlServerException(final Throwable throwable) {
    super(throwable);
  }
}
