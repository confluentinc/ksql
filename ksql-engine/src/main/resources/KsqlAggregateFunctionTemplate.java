package io.confluent.ksql.function.udaf;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.kstream.Merger;

import java.util.List;
import java.util.function.Supplier;

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.Udaf;

public class #FUNCTION_CLASS_NAME extends BaseAggregateFunction {

  private Udaf udaf;
  private int udafIndex;
  private final List<Schema> arguments;

  public #FUNCTION_CLASS_NAME(final List<Schema> args, final Schema returnType) {
    super("#NAME", -1, null, returnType, args);
    this.arguments = args;
  }

  public #FUNCTION_CLASS_NAME(final Udaf udaf,
                              final int udafIndex,
                              final List<Schema> args,
                              final Schema returnType) {
    super("#NAME", udafIndex, createSupplier(udaf), returnType, args);
    this.udaf = udaf;
    this.udafIndex = udafIndex;
  }

  @Override
  public KsqlAggregateFunction getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    return new #FUNCTION_CLASS_NAME(#CLASS.#METHOD(),
    aggregateFunctionArguments.udafIndex(),
    arguments,
    super.getReturnType());
  }

  @Override
  public Object aggregate(final Object currentValue, final Object aggregateValue) {
    return udaf.aggregate(currentValue, aggregateValue);
  }

  @Override
  public Merger getMerger() {
    return new Merger() {
      public Object apply(Object key, Object v1, Object v2) {
        return udaf.merge(v1, v2);
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