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

import java.util.concurrent.atomic.AtomicLong;

public class DefaultQueryIdGenerator implements QueryIdGenerator {

  private final AtomicLong queryIdCounter;

  public DefaultQueryIdGenerator() {
    this(0L);
  }

  public DefaultQueryIdGenerator(final long initialValue) {
    this.queryIdCounter = new AtomicLong(initialValue);
  }

  protected long getId() {
    return queryIdCounter.get();
  }

  @Override
  public String getNextId() {
    return String.valueOf(queryIdCounter.getAndIncrement());
  }

  @Override
  public QueryIdGenerator createSandbox() {
    return new DefaultQueryIdGenerator(queryIdCounter.get());
  }
}

