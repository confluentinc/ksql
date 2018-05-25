/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import org.junit.Test;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ExecutorWithRetriesTest {

  @Test
  public void shouldNotFailOnExistingResource() throws Exception{
    FakeKafkaTopicClient client = new FakeKafkaTopicClient();
    client.createTopic("foo", 1, (short) 1);
    ExecutorWithRetries.execute((Supplier<Future<Void>>)() -> {
      client.deleteTopics(Collections.singletonList("bar"));
      return CompletableFuture.completedFuture(null);
    });
    assertThat(client.isTopicExists("foo"), equalTo(true));
  }

  @Test
  public void shouldSuccedOnMissingResource() throws Exception{
    FakeKafkaTopicClient client = new FakeKafkaTopicClient();
    client.createTopic("foo", 1, (short) 1);
    ExecutorWithRetries.execute((Supplier<Future<Void>>)() -> {
      client.deleteTopics(Collections.singletonList("foo"));
      return CompletableFuture.completedFuture(null);
    });
    assertThat(client.isTopicExists("foo"), equalTo(false));
  }
}
