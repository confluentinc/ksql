package io.confluent.ksql.rest.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import io.confluent.ksql.rest.server.StandaloneExecutorFactory.StandaloneExecutorConstructor;
import io.confluent.ksql.rest.server.computation.ConfigStore;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StandaloneExecutorFactoryTest {
  private static final String QUERIES_FILE = "queries";
  private static final String INSTALL_DIR = "install";

  private final Map<String, String> properties = Collections.emptyMap();
  private final KsqlConfig baseConfig = new KsqlConfig(properties);
  private final KsqlConfig mergedConfig = new KsqlConfig(Collections.emptyMap());
  private final String configTopicName = KsqlInternalTopicUtils.getTopicName(
      baseConfig,
      StandaloneExecutorFactory.CONFIG_TOPIC_SUFFIX
  );

  @Mock
  private Function<KsqlConfig, ServiceContext> serviceContextFactory;
  @Mock
  private BiFunction<String, KsqlConfig, ConfigStore> configStoreFactory;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private ConfigStore configStore;
  @Mock
  private StandaloneExecutorConstructor constructor;
  @Mock
  private StandaloneExecutor standaloneExecutor;

  @Before
  public void setup() {
    when(serviceContextFactory.apply(any())).thenReturn(serviceContext);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(configStoreFactory.apply(any(), any())).thenReturn(configStore);
    when(topicClient.isTopicExists(configTopicName)).thenReturn(false);
    when(configStore.getKsqlConfig()).thenReturn(mergedConfig);
    when(constructor.create(any(), any(), any(), any(), anyString(), any(), anyBoolean(), any()))
        .thenReturn(standaloneExecutor);
  }

  private void create() {
    StandaloneExecutorFactory.create(
        properties,
        QUERIES_FILE,
        INSTALL_DIR,
        serviceContextFactory,
        configStoreFactory,
        constructor
    );
  }

  private Matcher<KsqlConfig> sameConfig(final KsqlConfig expected) {
    return new KsqlConfigMatcher(expected);
  }

  private static class KsqlConfigMatcher extends TypeSafeMatcher<KsqlConfig> {
    private final KsqlConfig expected;

    public KsqlConfigMatcher(final KsqlConfig expected) {
      this.expected = expected;
    }

    @Override
    public void describeTo(final Description description) {
      description.appendValue(expected.getAllConfigPropsWithSecretsObfuscated());
    }

    @Override
    public boolean matchesSafely(final KsqlConfig ksqlConfig) {
      return ksqlConfig.getAllConfigPropsWithSecretsObfuscated().equals(
          expected.getAllConfigPropsWithSecretsObfuscated());
    }
  }

  @Test
  public void shouldCreateConfigTopicThenGetConfig() {
    // When:
    create();

    // Then:
    final InOrder inOrder = Mockito.inOrder(topicClient, configStoreFactory, constructor);
    inOrder.verify(topicClient).createTopic(eq(configTopicName), anyInt(), anyShort(), anyMap());
    inOrder.verify(configStoreFactory).apply(eq(configTopicName), argThat(sameConfig(baseConfig)));
    inOrder.verify(constructor).create(
        any(), any(), same(mergedConfig), any(), anyString(), any(), anyBoolean(), any());
  }
}