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

package io.confluent.ksql.query.id;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Incrementally returns query Id identifier. The legacy method of generating a query Id.
 */
public class SequentialQueryIdGenerator implements QueryIdGenerator {

  private final AtomicLong queryIdCounter;

  public SequentialQueryIdGenerator() {
    this(0L);
  }

  SequentialQueryIdGenerator(final long initialValue) {
    this.queryIdCounter = new AtomicLong(initialValue);
  }

  @Override
  public String getNext() {
    return String.valueOf(queryIdCounter.getAndIncrement());
  }

  @Override
  public QueryIdGenerator createSandbox() {
    return new SequentialQueryIdGenerator(queryIdCounter.get());
  }
}

