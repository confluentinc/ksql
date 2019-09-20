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


public class HybridQueryIdGenerator implements QueryIdGenerator {
  private DefaultQueryIdGenerator defaultQueryIdGenerator;
  private QueryIdGeneratorUsingOffset queryIdGeneratorUsingOffset;
  private QueryIdGenerator primaryQueryIdGenerator;

  public HybridQueryIdGenerator() {
    this(0L);
  }

  public HybridQueryIdGenerator(final long initialValue) {
    this.defaultQueryIdGenerator = new DefaultQueryIdGenerator(initialValue);
    this.queryIdGeneratorUsingOffset = new QueryIdGeneratorUsingOffset();
    this.primaryQueryIdGenerator = this.defaultQueryIdGenerator;

  }

  public void updateOffset(final long offset) {
    this.primaryQueryIdGenerator = queryIdGeneratorUsingOffset;
    queryIdGeneratorUsingOffset.updateOffset(offset);
  }

  @Override
  public String getNextId() {
    return String.valueOf(primaryQueryIdGenerator.getNextId());
  }

  @Override
  public QueryIdGenerator createSandbox() {
    if (primaryQueryIdGenerator instanceof DefaultQueryIdGenerator) {
      return new HybridQueryIdGenerator(defaultQueryIdGenerator.getId());
    }

    return new HybridQueryIdGenerator(queryIdGeneratorUsingOffset.getId() + 1);
  }
}
