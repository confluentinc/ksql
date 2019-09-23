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

import com.google.common.annotations.VisibleForTesting;

/**
 * Contains both a Sequential Query Id Generator and a Specific QueryId Generator
 * Can toggle between the two depending on which one is currently activated
 * The query id generation method was changed from using an incremented counter
 * to using the offset of the record containing the query generating command.
 */
public class HybridQueryIdGenerator implements QueryIdGenerator {

  private final SequentialQueryIdGenerator legacyGenerator;
  private final SpecificQueryIdGenerator newGenerator;
  private QueryIdGenerator activeGenerator;

  public HybridQueryIdGenerator() {
    this(0L);
  }

  public HybridQueryIdGenerator(final long initialValue) {
    this.legacyGenerator = new SequentialQueryIdGenerator(initialValue);
    this.newGenerator = new SpecificQueryIdGenerator();
    this.activeGenerator = this.legacyGenerator;
  }

  @VisibleForTesting
  HybridQueryIdGenerator(
      final SequentialQueryIdGenerator sequentialQueryIdGenerator,
      final SpecificQueryIdGenerator specificQueryIdGenerator
  ) {
    this.legacyGenerator = sequentialQueryIdGenerator;
    this.newGenerator = specificQueryIdGenerator;
    this.activeGenerator = this.legacyGenerator;
  }

  public void activateNewGenerator(final long nextId) {
    newGenerator.setNextId(nextId);
    this.activeGenerator = newGenerator;
  }

  public void activateLegacyGenerator() {
    activeGenerator = legacyGenerator;
  }

  @Override
  public long peekNext() {
    return activeGenerator.peekNext();
  }

  @Override
  public String getNext() {
    return activeGenerator.getNext();
  }

  @Override
  public QueryIdGenerator createSandbox() {
    return new SequentialQueryIdGenerator(activeGenerator.peekNext());
  }
}
