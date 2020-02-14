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

package io.confluent.ksql.api.tck;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.plugin.BlockingQueryPublisher;
import io.confluent.ksql.api.server.PushQueryHandler;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public class BlockingQueryPublisherVerificationTest extends PublisherVerification<GenericRow> {

  private final Vertx vertx;
  private final WorkerExecutor workerExecutor;

  public BlockingQueryPublisherVerificationTest() {
    // We need to increase the default timeouts as they are a bit low and can lead to
    // non deterministic runs
    super(new TestEnvironment(1000), 1000);
    this.vertx = Vertx.vertx();
    this.workerExecutor = vertx.createSharedWorkerExecutor("test_workers");
  }

  @Override
  public Publisher<GenericRow> createPublisher(long elements) {
    final Context context = vertx.getOrCreateContext();
    BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);
    publisher.setQueryHandle(new TestQueryHandle(elements));
    if (elements < Integer.MAX_VALUE) {
      for (long l = 0; l < elements; l++) {
        publisher.accept(generateRow(l));
      }
    }
    return publisher;
  }

  @Override
  public Publisher<GenericRow> createFailedPublisher() {
    return null;
  }

  private GenericRow generateRow(long num) {
    List<Object> l = new ArrayList<>();
    l.add("foo" + num);
    l.add(num);
    l.add(num % 2 == 0);
    return GenericRow.fromList(l);
  }

  private static class TestQueryHandle implements PushQueryHandler {

    private final long elements;

    public TestQueryHandle(final long elements) {
      this.elements = elements;
    }

    @Override
    public List<String> getColumnNames() {
      return new ArrayList<>();
    }

    @Override
    public List<String> getColumnTypes() {
      return new ArrayList<>();
    }

    @Override
    public OptionalInt getLimit() {
      return elements != Long.MAX_VALUE ? OptionalInt.of((int) elements) : OptionalInt.empty();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {

    }
  }


}
