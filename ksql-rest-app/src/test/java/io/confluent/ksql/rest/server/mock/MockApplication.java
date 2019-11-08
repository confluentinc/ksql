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

package io.confluent.ksql.rest.server.mock;

import io.confluent.ksql.rest.server.ExecutableApplication;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.mock.MockStreamedQueryResource.TestStreamWriter;
import javax.ws.rs.core.Configurable;
import org.eclipse.jetty.server.NetworkTrafficServerConnector;
import org.glassfish.jersey.server.ServerProperties;


public class MockApplication extends ExecutableApplication<KsqlRestConfig> {

  private final MockStreamedQueryResource streamedQueryResource;

  public MockApplication(final KsqlRestConfig ksqlRestConfig) {
    super(ksqlRestConfig);
    streamedQueryResource = new MockStreamedQueryResource();
  }

  public MockStreamedQueryResource getStreamedQueryResource() {
    return streamedQueryResource;
  }

  @Override
  public void setupResources(final Configurable<?> configurable, final KsqlRestConfig ksqlRestConfig) {
    configurable.register(new MockKsqlResources());
    configurable.register(streamedQueryResource);
    configurable.register(new MockStatusResource());
    configurable.property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
  }

  @Override
  public void startAsync() {
  }

  @Override
  public void triggerShutdown() {
    for (TestStreamWriter testStreamWriter : streamedQueryResource.getWriters()) {
      try {
        testStreamWriter.finished();
      } catch (final Exception e) {
        System.err.println("Failed to finish stream writer");
        e.printStackTrace(System.err);
      }
    }
  }

  public String getServerAddress() {
    if (server == null) {
      throw new IllegalStateException("Not started");
    }

    final int localPort = ((NetworkTrafficServerConnector) server.getConnectors()[0])
        .getLocalPort();
    
    return "http://localhost:"+ localPort;
  }
}
