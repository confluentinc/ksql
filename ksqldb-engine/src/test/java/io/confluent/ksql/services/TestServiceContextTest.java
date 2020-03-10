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

package io.confluent.ksql.services;

import static org.mockito.Mockito.verify;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestServiceContextTest {

  @Mock
  private KafkaClientSupplier kafkaClientSupplier;
  @Mock
  private Admin adminClient;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private ConnectClient connectClient;

  @Test
  public void shouldCloseAdminClientOnClose() {
    // Given:
    final ServiceContext serviceContext = TestServiceContext.create(
        kafkaClientSupplier,
        adminClient,
        topicClient,
        srClientFactory,
        connectClient
    );

    // When:
    serviceContext.close();

    // Then:
    verify(adminClient).close();
  }
}