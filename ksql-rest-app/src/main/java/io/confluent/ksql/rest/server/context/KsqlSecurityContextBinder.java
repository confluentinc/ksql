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

import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.util.KsqlConfig;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;

/**
 * Configures the {@link KsqlSecurityContext} class for dependency injection using
 * the {@link javax.ws.rs.core.Context} annotation.
 * </p>
 * Inject {@code KsqlSecurityContext} on each REST method as follows:
 * i.e. myMethod(@Context KsqlSecurityContext securityContext)
 */
public class KsqlSecurityContextBinder extends AbstractBinder {
  public KsqlSecurityContextBinder(
      final KsqlConfig ksqlConfig,
      final KsqlSecurityExtension securityExtension
  ) {
    KsqlSecurityContextBinderFactory.configure(ksqlConfig, securityExtension);
  }

  @Override
  protected void configure() {
    bindFactory(KsqlSecurityContextBinderFactory.class)
        .to(KsqlSecurityContext.class)
        .in(RequestScoped.class);
  }
}
