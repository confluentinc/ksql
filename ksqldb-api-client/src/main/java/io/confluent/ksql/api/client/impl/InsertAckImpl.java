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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.InsertAck;
import java.util.Objects;

public class InsertAckImpl implements InsertAck {

  private final long num;

  InsertAckImpl(final long num) {
    this.num = num;
  }

  @Override
  public long seqNum() {
    return num;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final InsertAckImpl insertAck = (InsertAckImpl) o;
    return num == insertAck.num;
  }

  @Override
  public int hashCode() {
    return Objects.hash(num);
  }

  @Override
  public String toString() {
    return "InsertAckImpl{"
        + "num=" + num
        + '}';
  }
}
