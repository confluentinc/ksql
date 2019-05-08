/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.context;

import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;

import java.util.Objects;
import java.util.Optional;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.glassfish.hk2.api.Factory;

/**
 * This class implements {@link Factory}, which allows a REST application to create
 * a new {@link ServiceContext} during REST requests.
 */
public class KsqlRestServiceContextFactory implements Factory<ServiceContext> {
  private static KsqlConfig ksqlConfig;

  public static void configure(final KsqlConfig ksqlConfig) {
    KsqlRestServiceContextFactory.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
  }

  private final Optional<KsqlRestContext> ksqlRestContext;

  @Inject
  public KsqlRestServiceContextFactory(final ContainerRequestContext requestContext) {
    this.ksqlRestContext = KsqlRestContext.get(requestContext);
  }

  @Override
  public ServiceContext provide() {
    return DefaultServiceContext.create(ksqlConfig, getKafkaClientSupplier());
  }

  private KafkaClientSupplier getKafkaClientSupplier() {
    if (ksqlRestContext.isPresent()) {
      return new ConfiguredKafkaClientSupplier(
          new DefaultKafkaClientSupplier(),
          ksqlRestContext.get().getRestContextProperties()
      );
    }

    return new DefaultKafkaClientSupplier();
  }

  @Override
  public void dispose(final ServiceContext serviceContext) {
    serviceContext.close();
  }
}
