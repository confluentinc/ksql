/**
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

package io.confluent.ksql.rest.server.mock;

import javax.ws.rs.core.Configurable;

import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.rest.Application;
import org.glassfish.jersey.server.ServerProperties;


public class MockApplication extends Application<KsqlRestConfig> {
  private MockStreamedQueryResource streamedQueryResource;

  public MockApplication(KsqlRestConfig config) {
    super(config);
    streamedQueryResource = new MockStreamedQueryResource();
  }

  public MockStreamedQueryResource getStreamedQueryResource() {
    return streamedQueryResource;
  }

  @Override
  public void setupResources(Configurable<?> configurable, KsqlRestConfig ksqlRestConfig) {
    configurable.register(new MockKsqlResources());
    configurable.register(streamedQueryResource);
    configurable.register(new MockStatusResource());
    configurable.property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
  }
}
