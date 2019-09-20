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

public class QueryIdGeneratorUsingOffset implements QueryIdGenerator {

  private long offset;
  private boolean hasBeenUpdated;

  public QueryIdGeneratorUsingOffset() {
    this(0L);
  }

  private QueryIdGeneratorUsingOffset(final long offset) {
    this(offset, false);
  }

  private QueryIdGeneratorUsingOffset(final long offset, final boolean hasBeenUpdated) {
    this.offset = offset;
    this.hasBeenUpdated = hasBeenUpdated;
  }

  public void updateOffset(final long offset) {
    this.hasBeenUpdated = true;
    this.offset = offset;
  }

  protected long getId() {
    return offset;
  }

  @Override
  public String getNextId() {
    if (!hasBeenUpdated) {
      throw new RuntimeException("QueryIdGenerator has not been updated with new offset");
    }

    hasBeenUpdated = false;
    return String.valueOf(offset);
  }

  @Override
  public QueryIdGenerator createSandbox() {
    return new QueryIdGeneratorUsingOffset(offset, hasBeenUpdated);
  }
}
