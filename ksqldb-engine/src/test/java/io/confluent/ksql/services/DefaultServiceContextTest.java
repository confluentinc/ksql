/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.ksql.services;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.services.DefaultServiceContext.MemoizedSupplier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultServiceContextTest {

  @Mock
  private KafkaClientSupplier kafkaClientSupplier;
  @Mock
  private Admin adminClient;
  @Mock
  private Admin topicAdminClient;
  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private ConnectClient connectClient;
  @Mock
  private SimpleKsqlClient ksqlClient;

  private DefaultServiceContext serviceContext;

  @Before
  public void setUp() {
    serviceContext = new DefaultServiceContext(
        kafkaClientSupplier,
        () -> adminClient,
        () -> topicAdminClient,
        () -> srClient,
        () -> connectClient,
        () -> ksqlClient
    );
  }

  @Test
  public void shouldContinueClosingAfterAdminClientCloseThrows() {
    // Given: adminClient and ksqlClient have been initialized (topicAdminClient
    // is only initialized lazily by KafkaTopicClient operations, which we
    // intentionally skip — the bug is identical: any throw in close() must not
    // mask the closes that follow).
    serviceContext.getAdminClient();
    serviceContext.getKsqlClient();
    doThrow(new RuntimeException("transient broker error"))
        .when(adminClient).close();

    // When: close() runs and the first client throws.
    serviceContext.close();

    // Then: the remaining client is still closed — otherwise a transient
    // Admin.close() failure during shutdown leaks the ksql client's
    // connections, threads, and sockets.
    verify(adminClient).close();
    verify(ksqlClient).close();
  }

  @Test
  public void shouldNotPropagateExceptionFromClose() {
    // Given:
    serviceContext.getKsqlClient();
    doThrow(new RuntimeException("transient error")).when(ksqlClient).close();

    // When/Then: close() must not throw — JVM shutdown hooks and lifecycle
    // managers depend on close() completing.
    serviceContext.close();
  }

  @Test
  public void shouldCloseConnectClientToReleaseHttpClientResources() {
    // Given: the connect client has been requested (initializing its underlying
    // pooled HTTP client + evictor thread + SSL context).
    serviceContext.getConnectClient();

    // When:
    serviceContext.close();

    // Then: the connect client's close() runs — otherwise the HTTP connection
    // pool, evictor thread, and SSL context leak per ServiceContext.
    verify(connectClient).close();
  }

  @Test
  public void shouldNotCloseUninitializedClients() {
    // Given: no client has been requested.

    // When:
    serviceContext.close();

    // Then: close() must not lazily instantiate clients just to close them.
    verify(adminClient, org.mockito.Mockito.never()).close();
    verify(topicAdminClient, org.mockito.Mockito.never()).close();
    verify(ksqlClient, org.mockito.Mockito.never()).close();
  }

  @Test
  public void memoizedSupplierShouldNotMarkInitializedWhenSupplierThrows() {
    // Given: a supplier that throws on first call, then succeeds.
    final AtomicInteger calls = new AtomicInteger();
    final MemoizedSupplier<String> supplier = new MemoizedSupplier<>(() -> {
      if (calls.incrementAndGet() == 1) {
        throw new RuntimeException("first call fails");
      }
      return "ok";
    });

    // When: first call throws.
    assertThrows(RuntimeException.class, supplier::get);

    // Then: the supplier must NOT report itself as initialized — otherwise
    // close() would call get() again, either creating a fresh resource just to
    // immediately close it, or re-throwing the same error and masking the
    // original shutdown cause.
    assertThat(supplier.isInitialized(), is(false));
  }

  @Test
  public void memoizedSupplierShouldMarkInitializedAfterSuccessfulGet() {
    final MemoizedSupplier<String> supplier = new MemoizedSupplier<>(() -> "ok");

    assertThat(supplier.isInitialized(), is(false));
    assertThat(supplier.get(), is("ok"));
    assertThat(supplier.isInitialized(), is(true));
  }
}
