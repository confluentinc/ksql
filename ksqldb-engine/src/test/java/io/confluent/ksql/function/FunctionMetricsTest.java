package io.confluent.ksql.function;

import static io.confluent.ksql.function.FunctionMetrics.AVG_DESC;
import static io.confluent.ksql.function.FunctionMetrics.COUNT_DESC;
import static io.confluent.ksql.function.FunctionMetrics.MAX_DESC;
import static io.confluent.ksql.function.FunctionMetrics.RATE_DESC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FunctionMetricsTest {

  private static final String SENSOR_NAME = "sensorName";
  private static final String GROUP_NAME = "groupName";
  private static final String FUNC_NAME = "Func name";

  @Mock
  private Metrics metrics;
  @Mock
  private Sensor sensor;
  @Mock
  private MetricName metricName;
  @Mock
  private MetricName specificMetricName;

  @Before
  public void setup() {
    when(metrics.sensor(any())).thenReturn(sensor);

    when(metrics.metricName(any(String.class), any(String.class), any(String.class)))
        .thenReturn(metricName);
  }

  @Test
  public void shouldGetSensorWithCorrectName() {
    // When:
    FunctionMetrics
        .getInvocationSensor(metrics, SENSOR_NAME, GROUP_NAME, FUNC_NAME);

    // Then:
    verify(metrics).sensor(SENSOR_NAME);
  }

  @Test
  public void shouldReturnSensorOnFirstCall() {
    // When:
    final Sensor result = FunctionMetrics
        .getInvocationSensor(metrics, SENSOR_NAME, GROUP_NAME, FUNC_NAME);

    // Then:
    assertThat(result, is(sensor));
  }

  @Test
  public void shouldReturnSensorOnSubsequentCalls() {
    // Given:
    when(sensor.hasMetrics()).thenReturn(true);

    // When:
    final Sensor result = FunctionMetrics
        .getInvocationSensor(metrics, SENSOR_NAME, GROUP_NAME, FUNC_NAME);

    // Then:
    assertThat(result, is(sensor));
  }

  @Test
  public void shouldRegisterAvgMetric() {
    // Given:
    when(metrics.metricName(SENSOR_NAME + "-avg", GROUP_NAME, description(AVG_DESC)))
        .thenReturn(specificMetricName);

    // When:
    FunctionMetrics
        .getInvocationSensor(metrics, SENSOR_NAME, GROUP_NAME, FUNC_NAME);

    // Then:
    verify(sensor).add(eq(specificMetricName), isA(Avg.class));
  }

  @Test
  public void shouldRegisterMaxMetric() {
    // Given:
    when(metrics.metricName(SENSOR_NAME + "-max", GROUP_NAME, description(MAX_DESC)))
        .thenReturn(specificMetricName);

    // When:
    FunctionMetrics
        .getInvocationSensor(metrics, SENSOR_NAME, GROUP_NAME, FUNC_NAME);

    // Then:
    verify(sensor).add(eq(specificMetricName), isA(Max.class));
  }

  @Test
  public void shouldRegisterCountMetric() {
    // Given:
    when(metrics.metricName(SENSOR_NAME + "-count", GROUP_NAME, description(COUNT_DESC)))
        .thenReturn(specificMetricName);

    // When:
    FunctionMetrics
        .getInvocationSensor(metrics, SENSOR_NAME, GROUP_NAME, FUNC_NAME);

    // Then:
    verify(sensor).add(eq(specificMetricName), isA(WindowedCount.class));
  }

  @Test
  public void shouldRegisterRateMetric() {
    // Given:
    when(metrics.metricName(SENSOR_NAME + "-rate", GROUP_NAME, description(RATE_DESC)))
        .thenReturn(specificMetricName);

    // When:
    FunctionMetrics
        .getInvocationSensor(metrics, SENSOR_NAME, GROUP_NAME, FUNC_NAME);

    // Then:
    verify(sensor).add(eq(specificMetricName), isA(Rate.class));
  }

  @Test
  public void shouldNotInitializeOnSubsequentCalls() {
    // Given:
    when(sensor.hasMetrics()).thenReturn(true);

    // When:
    FunctionMetrics
        .getInvocationSensor(metrics, SENSOR_NAME, GROUP_NAME, FUNC_NAME);

    // Then:
    verify(sensor, never()).add(any(MetricName.class), any());
  }

  private static String description(final String formatString) {
    return String.format(formatString, FUNC_NAME);
  }
}