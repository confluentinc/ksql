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

/**
 * Represents a response to an insert
 */
public class InsertResponse {

  public final String status;
  public final Integer errorCode;
  public final String message;

  public InsertResponse(final Integer errorCode, final String message) {
    this.status = "error";
    this.errorCode = errorCode;
    this.message = message;
  }

  public InsertResponse() {
    this.status = "ok";
    this.errorCode = null;
    this.message = null;
  }

  @Override
  public String toString() {
    return "AckResponse{"
        + "status='" + status + '\''
        + ", errorCode=" + errorCode
        + ", message='" + message + '\''
        + '}';
  }
}
