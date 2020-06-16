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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.errorprone.annotations.Immutable;

/**
 * Represents a response to an insert
 */
@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class InsertAck {

  public final long seq;
  public final String status;

  public InsertAck(final long seq) {
    this.seq = seq;
    this.status = "ok";
  }

  @Override
  public String toString() {
    return "InsertAck{"
        + "seq=" + seq
        + ", status='" + status + '\''
        + '}';
  }
}
