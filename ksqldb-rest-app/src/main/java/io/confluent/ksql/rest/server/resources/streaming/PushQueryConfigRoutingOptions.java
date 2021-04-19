package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.ksql.physical.scalable_push.PushRoutingOptions;
import io.confluent.ksql.util.KsqlRequestConfig;
import java.util.Map;
import java.util.Objects;

public class PushQueryConfigRoutingOptions implements PushRoutingOptions {

  private final Map<String, ?> requestProperties;

  public PushQueryConfigRoutingOptions(
      final Map<String, ?> requestProperties
  ) {
    this.requestProperties = Objects.requireNonNull(requestProperties, "requestProperties");
  }

  @Override
  public boolean getIsSkipForwardRequest() {
    if (requestProperties.containsKey(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING)) {
      return (Boolean) requestProperties.get(
          KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING);
    }
    return KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING_DEFAULT;
  }
}
