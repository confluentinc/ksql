/*
 * Copyright 2021 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

/**
 * Represents an error on an insert stream
 */
@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class InsertError extends KsqlErrorMessage {

  public final String status;
  public final long seq;

  public InsertError(final long seq, final int errorCode, final String message) {
    super(errorCode, message);
    this.seq = seq;
    this.status = "error";
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlErrorMessage that = (KsqlErrorMessage) o;
    return getErrorCode() == that.getErrorCode()
        && Objects.equals(that.getMessage(), that.getMessage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), status, seq);
  }
}
