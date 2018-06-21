package io.confluent.ksql.function.udaf;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.concurrent.TimeUnit;

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.Udaf;

/**
 * This is the template used to generate UDAF classes that are built from
 * the ext directory. Each UDAF will be compiled into one of these
 * classes.
 *
 * Note: Java 7 style as the compiler doesn't support lambdas
 */
public class #FUNCTION_CLASS_NAME extends BaseAggregateFunction {

  private Udaf udaf;
  private int udafIndex;
  private final List<Schema> arguments;
  private final Time time = Time.SYSTEM;
  private final Optional<Sensor> aggregateSensor;
  private final Optional<Sensor> mergeSensor;

  public #FUNCTION_CLASS_NAME(final List<Schema> args,
                              final Schema returnType,
                              final Optional<Metrics> metrics) {
    super("#NAME", -1, null, returnType, args);
    this.arguments = args;
    if (metrics.isPresent()) {
      final Metrics theMetrics = (Metrics)metrics.get();
      final String aggSensorName = "ksql-udaf-aggregate-#NAME-#METHOD";
      if(theMetrics.getSensor(aggSensorName) == null) {
          final Sensor sensor = theMetrics.sensor(aggSensorName);
          sensor.add(theMetrics.metricName(aggSensorName + "-avg", aggSensorName,
          "Average time for an aggregate invocation of #NAME #METHOD udaf"),
          new Avg());
          sensor.add(theMetrics.metricName(aggSensorName + "-max", aggSensorName,
          "Max time for an aggregate invocation of #NAME #METHOD udaf"),
          new Max());
          sensor.add(theMetrics.metricName(aggSensorName + "-count", aggSensorName,
          "Total number of aggregate invocations of #NAME #METHOD udaf"),
          new Count());
          sensor.add(theMetrics.metricName(aggSensorName + "-rate", aggSensorName,
          "The average number of occurrences of aggregate #NAME #METHOD operation per second udaf"),
          new Rate(TimeUnit.SECONDS, new Count()));
      }
      final String mergeSensorName = "ksql-udaf-merge-#NAME-#METHOD";
      if(theMetrics.getSensor(mergeSensorName) == null) {
        final Sensor sensor = theMetrics.sensor(mergeSensorName);
        sensor.add(theMetrics.metricName(mergeSensorName + "-avg", mergeSensorName,
        "Average time for a merge invocation of #NAME #METHOD udaf"),
        new Avg());
        sensor.add(theMetrics.metricName(mergeSensorName + "-max", mergeSensorName,
        "Max time for a merge invocation of #NAME #METHOD udaf"),
        new Max());
        sensor.add(theMetrics.metricName(mergeSensorName + "-count", mergeSensorName,
        "Total number of merge invocations of #NAME #METHOD udaf"),
        new Count());
        sensor.add(theMetrics.metricName(mergeSensorName + "-rate", mergeSensorName,
        "The average number of occurrences of merge #NAME #METHOD operation per second udaf"),
        new Rate(TimeUnit.SECONDS, new Count()));
      }
      this.aggregateSensor = Optional.of(theMetrics.getSensor(aggSensorName));
      this.mergeSensor = Optional.of(theMetrics.getSensor(mergeSensorName));
    } else {
      aggregateSensor = Optional.empty();
      mergeSensor = Optional.empty();
    }
  }

  private #FUNCTION_CLASS_NAME(final Udaf udaf,
                              final int udafIndex,
                              final List<Schema> args,
                              final Schema returnType,
                              final Optional<Sensor> aggregateSensor,
                              final Optional<Sensor> mergeSensor) {
    super("#NAME", udafIndex, createSupplier(udaf), returnType, args);
    this.udaf = udaf;
    this.udafIndex = udafIndex;
    this.aggregateSensor = aggregateSensor;
    this.mergeSensor = mergeSensor;
  }

  @Override
  public KsqlAggregateFunction getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    aggregateFunctionArguments.ensureArgCount(#ARG_COUNT, "#NAME");
    return new #FUNCTION_CLASS_NAME(#CLASS.#METHOD(#ARGS),
    aggregateFunctionArguments.udafIndex(),
    arguments,
    super.getReturnType(),
    aggregateSensor,
    mergeSensor);
  }

  @Override
  public Object aggregate(final Object currentValue, final Object aggregateValue) {
    final long start = time.nanoseconds();
    try {
      return udaf.aggregate(currentValue,aggregateValue);
    } finally {
      aggregateSensor.ifPresent(new Consumer() {
        public void accept(final Object sensor) {
        ((Sensor)sensor).record(time.nanoseconds() - start);
        }
      });
    }
  }

  @Override
  public Merger getMerger(){
    return new Merger(){
      public Object apply(Object key,Object v1,Object v2){
        final long start = time.nanoseconds();
        try{
          return udaf.merge(v1,v2);
        }finally{
          mergeSensor.ifPresent(new Consumer(){
            public void accept(final Object sensor){
              ((Sensor)sensor).record(time.nanoseconds()-start);
            }
          });
        }
      }
    };
  }

  private static Supplier createSupplier(final Udaf udaf) {
    return new Supplier() {
        public Object get() {
          return udaf.initialize();
        }
      };
    }
}