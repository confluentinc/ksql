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
   * Registers and initializes the resource extension with additional server context (metrics and
   * node/cluster identity). The default delegates to {@link #register(KsqlConfig)} so extensions
   * that do not need the extra context keep working unchanged.
   * @param context the registration context
   */
  default void register(final KsqlResourceExtensionContext context) {
    register(context.ksqlConfig());
  }

  /**
   * Closes the resource extension and releases any held resources.
   */
  @Override
  void close();
}
