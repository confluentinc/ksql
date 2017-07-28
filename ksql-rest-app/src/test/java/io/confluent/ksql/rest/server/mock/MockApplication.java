package io.confluent.ksql.rest.server.mock;

import javax.ws.rs.core.Configurable;

import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.rest.Application;


public class MockApplication extends Application<KsqlRestConfig> {

  public MockApplication(KsqlRestConfig config) {
    super(config);
  }

  @Override
  public void setupResources(Configurable<?> configurable, KsqlRestConfig ksqlRestConfig) {
    configurable.register(new MockKsqlResources());
    configurable.register(new MockStreamedQueryResource());
    configurable.register(new MockStatusResource());
  }
}
