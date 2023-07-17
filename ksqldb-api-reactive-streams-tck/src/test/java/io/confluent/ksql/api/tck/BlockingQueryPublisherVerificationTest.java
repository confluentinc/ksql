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
import io.confluent.ksql.api.impl.BlockingQueryPublisher;
import io.confluent.ksql.api.server.QueryHandle;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.util.KeyValue;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public class BlockingQueryPublisherVerificationTest extends PublisherVerification<KeyValue<List<?>, GenericRow>> {

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
  public Publisher<KeyValue<List<?>, GenericRow>> createPublisher(long elements) {
    final Context context = vertx.getOrCreateContext();
    BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);
    final TestQueryHandle queryHandle = new TestQueryHandle(elements);
    publisher.setQueryHandle(queryHandle, false);
    if (elements < Integer.MAX_VALUE) {
      for (long l = 0; l < elements; l++) {
        queryHandle.queue.acceptRow(null, generateRow(l));
      }
    }
    return publisher;
  }

  @Override
  public Publisher<KeyValue<List<?>, GenericRow>> createFailedPublisher() {
    return null;
  }

  private static GenericRow generateRow(long num) {
    List<Object> l = new ArrayList<>();
    l.add("foo" + num);
    l.add(num);
    l.add(num % 2 == 0);
    return GenericRow.fromList(l);
  }

  private static class TestQueryHandle implements QueryHandle {

    private final TransientQueryQueue queue;

    public TestQueryHandle(final long elements) {
      final OptionalInt limit = elements == Long.MAX_VALUE
          ? OptionalInt.empty()
          : OptionalInt.of((int) elements);

      this.queue = new TransientQueryQueue(limit);
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
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public BlockingRowQueue getQueue() {
      return queue;
    }

    @Override
    public void onException(Consumer<Throwable> onException) {
    }
  }
}
