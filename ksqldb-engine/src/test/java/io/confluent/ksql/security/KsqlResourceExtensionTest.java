/*
 * Copyright 2026 Confluent Inc.
 */

package io.confluent.ksql.security;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.util.KsqlConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlResourceExtensionTest {

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private MetricCollectors metricCollectors;

  @Test
  public void shouldDelegateContextRegistrationToKsqlConfigOverloadByDefault() {
    // Given: an extension that only implements the legacy register(KsqlConfig)
    final RecordingExtension extension = new RecordingExtension();

    // When: registered with the richer context
    extension.register(context());

    // Then: the default hook forwards the config to the legacy overload
    assertThat(extension.registeredConfig, is(sameInstance(ksqlConfig)));
  }

  @Test
  public void shouldPassFullContextToContextAwareExtension() {
    // Given: an extension that overrides the context-aware overload
    final ContextAwareExtension extension = new ContextAwareExtension();
    final KsqlResourceExtensionContext context = context();

    // When
    extension.register(context);

    // Then: the extension receives the context as-is
    assertThat(extension.registeredContext, is(sameInstance(context)));
  }

  private KsqlResourceExtensionContext context() {
    return new KsqlResourceExtensionContext(ksqlConfig, metricCollectors, "node-1", "cluster-1");
  }

  private static final class RecordingExtension implements KsqlResourceExtension {
    private KsqlConfig registeredConfig;

    @Override
    public void register(final KsqlConfig config) {
      this.registeredConfig = config;
    }

    @Override
    public void close() {
    }
  }

  private static final class ContextAwareExtension implements KsqlResourceExtension {
    private KsqlResourceExtensionContext registeredContext;

    @Override
    public void register(final KsqlConfig config) {
    }

    @Override
    public void register(final KsqlResourceExtensionContext context) {
      this.registeredContext = context;
    }

    @Override
    public void close() {
    }
  }
}
