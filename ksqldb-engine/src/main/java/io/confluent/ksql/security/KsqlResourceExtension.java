/*
 * Copyright 2025 Confluent Inc.
 */

package io.confluent.ksql.security;

import io.confluent.ksql.util.KsqlConfig;

/**
 * Interface for extending ksqlDB with additional resource functionality.
 */
public interface KsqlResourceExtension extends AutoCloseable {
  
  /**
   * Registers and initializes the resource extension.
   * @param ksqlConfig the ksqlDB configuration containing all server settings
   * @throws Exception if the extension cannot be properly initialized
   */
  void register(KsqlConfig ksqlConfig);

  /**
   * Closes the resource extension and releases any held resources.
   */
  @Override
  void close();
}
