/*
 * Copyright 2025 Confluent Inc.
 */

package io.confluent.ksql.security;

import io.confluent.ksql.util.KsqlConfig;

public interface KsqlResourceExtension {
  void register(KsqlConfig ksqlConfig);

  void clean();
}
