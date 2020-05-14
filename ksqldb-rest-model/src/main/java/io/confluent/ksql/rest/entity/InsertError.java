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

package io.confluent.ksql.rest.entity;

import com.google.errorprone.annotations.Immutable;

/**
 * Represents an error on an insert stream
 */
@Immutable
public class InsertError extends KsqlErrorMessage {

  public final String status = "error";
  public final long seq;

  public InsertError(final long seq, final int errorCode, final String message) {
    super(errorCode, message);
    this.seq = seq;
  }
}
