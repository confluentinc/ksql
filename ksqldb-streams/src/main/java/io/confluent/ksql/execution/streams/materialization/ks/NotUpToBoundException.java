/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.streams.materialization.ks;

import io.confluent.ksql.util.KsqlException;

/**
 * This exception is thrown to indicate that a pull query failed the consistency check on
 * this host and should fallback on the next standby in line.
 */
public class NotUpToBoundException extends KsqlException {

  public NotUpToBoundException(final Throwable cause) {
    super(cause);
  }

  public NotUpToBoundException(final String message) {
    super(message);
  }

  public NotUpToBoundException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
