/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.mock;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.mock.MockStreamedQueryResource.TestStreamWriter;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.Configurable;
import org.apache.kafka.streams.StreamsConfig;
import org.eclipse.jetty.server.NetworkTrafficServerConnector;
import org.glassfish.jersey.server.ServerProperties;


public class MockApplication extends Application<KsqlRestConfig> {

  private final MockStreamedQueryResource streamedQueryResource;

  public MockApplication() {
    super(createConfig());
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
  public void stop() {
    for (TestStreamWriter testStreamWriter : streamedQueryResource.getWriters()) {
      try {
        testStreamWriter.finished();
      } catch (final Exception e) {
        System.err.println("Failed to finish stream writer");
        e.printStackTrace(System.err);
      }
    }

   try {
     super.stop();
   } catch (final Exception e) {
     System.err.println("Failed to stop app");
     e.printStackTrace(System.err);
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

  private static KsqlRestConfig createConfig() {
    final Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .put(StreamsConfig.APPLICATION_ID_CONFIG, "ksql_config_test")
        .put(RestConfig.SHUTDOWN_GRACEFUL_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(30))
        .build();

    return new KsqlRestConfig(props);
  }
}
