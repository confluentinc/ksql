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
import org.glassfish.hk2.api.Factory;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures the {@link ServiceContext} class for dependency injection using the
 * {@link javax.ws.rs.core.Context} annotation.
 * </p>
 * This class does not use an impersonated ServiceContext. It is used only to inject
 * a default ServiceContext on each REST request.
 * </p>
 * Inject {@code ServiceContext} on each REST method as follows:
 * i.e. myMethod(@Context ServiceContext serviceContext)
 */
public class KsqlDisableImpersonationHandler extends KsqlImpersonationHandler {
  private static final Logger log = LoggerFactory.getLogger(KsqlDisableImpersonationHandler.class);

  public KsqlDisableImpersonationHandler(final KsqlConfig ksqlConfig) {
    super(ksqlConfig);
  }

  @Override
  public void configure() {
    bindFactory(new Factory<ServiceContext>() {
      @Override
      public ServiceContext provide() {
        return DefaultServiceContext.create(ksqlConfig);
      }

      @Override
      public void dispose(final ServiceContext serviceContext) {
        if (serviceContext != null) {
          serviceContext.close();
        }
      }
    }).to(ServiceContext.class).in(RequestScoped.class);

    log.info("KSQL impersonation disabled.");
  }
}
