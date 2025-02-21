package io.confluent.ksql.security.oauth;

import org.apache.kafka.common.Configurable;

import java.util.Map;

public interface IdpConfig extends Configurable {
  Map<String, Object> getIdpConfigs();

}
