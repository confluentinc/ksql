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

package io.confluent.ksql.api.server.protocol;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

/**
 * Represents an error on an insert stream
 */
@Immutable
public class InsertError extends SerializableObject {

  public final long seq;
  public final String status;
  public final int errorCode;
  public final String message;

  public InsertError(final long seq, final int errorCode, final String message) {
    this.seq = seq;
    this.status = "error";
    this.errorCode = errorCode;
    this.message = Objects.requireNonNull(message);
  }

  @Override
  public String toString() {
    return "ErrorResponse{"
        + "seq=" + seq
        + "status='" + status + '\''
        + ", errorCode=" + errorCode
        + ", message='" + message + '\''
        + '}';
  }
}
