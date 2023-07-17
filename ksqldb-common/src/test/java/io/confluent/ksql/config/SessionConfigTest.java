package io.confluent.ksql.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
@RunWith(MockitoJUnitRunner.class)
public class SessionConfigTest {

  private static final Map<String, Integer> OVERRIDES = ImmutableMap.of("Something", 1);

  @Mock
  private KsqlConfig systemConfig;
  @Mock
  private KsqlConfig configWithOverrides;
  private SessionConfig config;

  @Before
  public void setUp() {
    when(systemConfig.cloneWithPropertyOverwrite(any())).thenReturn(configWithOverrides);

    config = SessionConfig.of(systemConfig, OVERRIDES);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            SessionConfig.of(systemConfig, OVERRIDES),
            SessionConfig.of(systemConfig, OVERRIDES)
        )
        .addEqualityGroup(
            SessionConfig.of(new KsqlConfig(ImmutableMap.of()), OVERRIDES)
        )
        .addEqualityGroup(
            SessionConfig.of(systemConfig, ImmutableMap.of("different", "overrides"))
        )
        .testEquals();
  }

  @Test
  public void shouldHaveSensibleToString() {
    // When:
    final String result = SessionConfig.of(systemConfig, OVERRIDES).toString();

    // Then:
    assertThat(result, containsString(systemConfig.toString()));
    assertThat(result, containsString(OVERRIDES.toString()));
  }

  @Test
  public void shouldGetConfigWithoutOverrides() {
    // When:
    final KsqlConfig result = config.getConfig(false);

    // Then:
    assertThat(result, is(sameInstance(systemConfig)));
  }

  @Test
  public void shouldGetConfigWithOverrides() {
    // When:
    final KsqlConfig result = config.getConfig(true);

    // Then:
    assertThat(result, is(sameInstance(configWithOverrides)));
    verify(systemConfig).cloneWithPropertyOverwrite(OVERRIDES);
  }

  @Test
  public void shouldTakeDefensiveCopyOfProperties() {
    // Given:
    final Map<String, Object> props = new HashMap<>();
    props.put("this", "that");

    final SessionConfig config = SessionConfig.of(systemConfig, props);

    // When:
    props.put("other", "thing");

    // Then:
    assertThat(config.getOverrides(), is(ImmutableMap.of("this", "that")));
  }

  @Test
  public void shouldReturnImmutableProperties() {
    // Given:
    final SessionConfig config = SessionConfig.of(systemConfig, new HashMap<>());

    // Then:
    assertThat(config.getOverrides(), is(instanceOf(ImmutableMap.class)));
  }

  @Test
  public void shouldCopyWithExistingConfig() {
    // Given:
    final SessionConfig config = SessionConfig.of(systemConfig,
        ImmutableMap.of("key1", 1, "key2", "value2"));

    // When:
    final SessionConfig newConfig = config.copyWith(
        ImmutableMap.of("key3", Arrays.asList(1, 2, 3)));

    // Then:
    assertThat(newConfig.getOverrides(),
        is(ImmutableMap.of("key1", 1, "key2", "value2", "key3", Arrays.asList(1, 2, 3))));
    assertThat(newConfig.getConfig(false), is(config.getConfig(false)));
  }

  @Test
  public void shouldCreateWithNewConfig() {
    // Given:
    final SessionConfig config = SessionConfig.of(systemConfig,
        ImmutableMap.of("key1", 1, "key2", "value2"));

    // When:
    final SessionConfig newConfig = config.withNewOverrides(
        ImmutableMap.of("key3", Arrays.asList(1, 2, 3)));

    // Then:
    assertThat(newConfig.getOverrides(),
        is(ImmutableMap.of("key3", Arrays.asList(1, 2, 3))));
    assertThat(newConfig.getConfig(false), is(config.getConfig(false)));
  }
}