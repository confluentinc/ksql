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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Contains both a Sequential Query Id Generator and a Specific QueryId Generator
 * Can toggle between the two depending on which one is currently activated
 * The query id generation method was changed from using an incremented counter
 * to using the offset of the record containing the query generating command.
 */
public class HybridQueryIdGenerator implements QueryIdGenerator {

  private final SpecificQueryIdGenerator newGenerator;
  private final AtomicReference<QueryIdGenerator> activeGenerator;

  public HybridQueryIdGenerator() {
    this(
        new SequentialQueryIdGenerator(),
        new SpecificQueryIdGenerator()
    );
  }

  @VisibleForTesting
  HybridQueryIdGenerator(
      final SequentialQueryIdGenerator sequentialQueryIdGenerator,
      final SpecificQueryIdGenerator specificQueryIdGenerator
  ) {
    this.newGenerator = specificQueryIdGenerator;
    this.activeGenerator = new AtomicReference<>(sequentialQueryIdGenerator);
  }

  public void activateNewGenerator(final long nextId) {
    newGenerator.setNextId(nextId);
    activeGenerator.set(newGenerator);
  }

  @Override
  public String getNext() {
    return activeGenerator.get().getNext();
  }

  @Override
  public QueryIdGenerator createSandbox() {
    return activeGenerator.get().createSandbox();
  }
}
