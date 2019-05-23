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

package io.confluent.ksql.rest.server.state;

import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.ext.Provider;

@Provider
public class ServerStateDynamicBinding implements DynamicFeature {
  private final ServerState state;

  public ServerStateDynamicBinding(final ServerState state) {
    this.state = state;
  }

  @Override
  public void configure(final ResourceInfo resourceInfo, final FeatureContext context) {
    final String packageName = resourceInfo.getResourceClass().getPackage().getName();
    if (packageName.startsWith("io.confluent.ksql.rest")) {
      context.register(new ServerStateFilter(state));
    }
  }
}
