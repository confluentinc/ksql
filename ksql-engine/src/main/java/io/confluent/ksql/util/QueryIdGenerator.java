/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class QueryIdGenerator {

  private final AtomicLong queryIdCounter = new AtomicLong(0);
  private final String postfix;

  public QueryIdGenerator(final String postfix) {
    this.postfix = Objects.requireNonNull(postfix, "postfix");
  }

  public String getNextId() {
    return queryIdCounter.getAndIncrement() + postfix;
  }
}
