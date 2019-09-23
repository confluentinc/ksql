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

import io.confluent.ksql.util.KsqlServerException;

/**
 * Returns a specific query Id identifier based on what's set. Only returns each set Id once and
 * will throw an exception if getNextId() is called twice without update.
 */
public class SpecificQueryIdGenerator implements QueryIdGenerator {

  private long nextId;
  private boolean alreadyUsed;

  public SpecificQueryIdGenerator() {
    this.nextId = 0L;
    this.alreadyUsed = true;
  }

  public void setNextId(final long nextId) {
    alreadyUsed = false;
    this.nextId = nextId;
  }

  @Override
  public long peekNext() {
    return alreadyUsed ? nextId + 1 : nextId;
  }

  @Override
  public String getNext() {
    if (alreadyUsed) {
      throw new KsqlServerException("QueryIdGenerator has not been updated with new offset");
    }

    alreadyUsed = true;
    return String.valueOf(nextId);
  }

  @Override
  public QueryIdGenerator createSandbox() {
    // We don't expect to have this function called for this type of generator
    throw new UnsupportedOperationException();
  }
}
