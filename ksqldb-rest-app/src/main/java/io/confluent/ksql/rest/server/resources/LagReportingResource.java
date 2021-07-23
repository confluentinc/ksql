/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.LagReportingResponse;
import io.confluent.ksql.rest.server.LagReportingAgent;

public class LagReportingResource {

  private final LagReportingAgent lagReportingAgent;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public LagReportingResource(final LagReportingAgent lagReportingAgent) {
    this.lagReportingAgent = lagReportingAgent;
  }

  public EndpointResponse receiveHostLag(final LagReportingMessage request) {
    lagReportingAgent.receiveHostLag(request);
    return EndpointResponse.ok(new LagReportingResponse(true));
  }
}
