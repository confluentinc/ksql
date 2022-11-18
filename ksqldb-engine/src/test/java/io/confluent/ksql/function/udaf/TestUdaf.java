/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
    name = "test_udaf",
    description = "test_udaf",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public final class TestUdaf {

  private TestUdaf() {
  }

  @UdafFactory(description = "sums longs")
  public static TableUdaf<Long, Long, Long> createSumLong() {
    return new TableUdaf<Long, Long, Long>() {
      @Override
      public Long undo(final Long valueToUndo, final Long aggregateValue) {
        return aggregateValue - valueToUndo;
      }

      @Override
      public Long initialize() {
        return 0L;
      }

      @Override
      public Long aggregate(final Long value, final Long aggregate) {
        return aggregate + value;
      }

      @Override
      public Long merge(final Long aggOne, final Long aggTwo) {
        return aggOne + aggTwo;
      }

      @Override
      public Long map(final Long agg) {
        return agg;
      }
    };
  }

  @UdafFactory(description = "sums int")
  public static TableUdaf<Integer, Long,Long> createSumInt() {
    return new SumIntUdaf();
  }

  @UdafFactory(description = "sums double")
  public static Udaf<Double, Double, Double> createSumDouble() {
    return new Udaf<Double, Double, Double>() {
      @Override
      public Double initialize() {
        return 0.0;
      }

      @Override
      public Double aggregate(final Double val, final Double aggregate) {
        return aggregate + val;
      }

      @Override
      public Double merge(final Double aggOne, final Double aggTwo) {
        return aggOne + aggTwo;
      }

      @Override
      public Double map(final Double agg) {
        return agg;
      }
    };
  }

  @UdafFactory(description = "sums numbers")
  public static <T extends Number> Udaf<T, T, T> createSumT() {
    return new Udaf<T, T, T>() {
      Udaf<T, T, T> delegate = null;
      SqlType type;

      @Override
      public T initialize() {
        return delegate.initialize();
      }

      @Override
      public T aggregate(T current, T aggregate) {
        return delegate.aggregate(current, aggregate);
      }

      @Override
      public T merge(T aggOne, T aggTwo) {
        return delegate.merge(aggOne, aggTwo);
      }

      @Override
      public T map(T agg) {
        return delegate.map(agg);
      }

      @Override
      public void initializeTypeArguments(List<SqlArgument> argTypeList) {
        type = argTypeList.get(0).getSqlTypeOrThrow();
        if (type == SqlTypes.DOUBLE) {
          delegate = (Udaf<T, T, T>) createSumDouble();
        } else if (type == SqlTypes.INTEGER) {
          delegate = (Udaf<T, T, T>) createSumInt();
        } else if (type == SqlTypes.BIGINT) {
          delegate = (Udaf<T, T, T>) createSumLong();
        } else {
          throw new IllegalArgumentException("Type " + type + " is not handled.");
        }
      }

      @Override
      public Optional<SqlType> getAggregateSqlType() {
        return Optional.of(type);
      }

      @Override
      public Optional<SqlType> getReturnSqlType() {
        return Optional.of(type);
      }
    };
  }

  @UdafFactory(description = "sums the length of strings")
  public static Udaf<String, Long, Long> createSumLengthString(final String initialString) {
    return new Udaf<String, Long, Long>() {
      @Override
      public Long initialize() {
        return (long) initialString.length();
      }

      @Override
      public Long aggregate(final String s, final Long aggregate) {
        return aggregate + s.length();
      }

      @Override
      public Long merge(final Long aggOne, final Long aggTwo) {
        return aggOne + aggTwo;
      }

      @Override
      public Long map(final Long agg) {
        return agg;
      }
    };
  }

  @UdafFactory(
      description = "returns a struct with {SUM(in->A), SUM(in->B)}",
      paramSchema = "STRUCT<A INTEGER, B INTEGER>",
      aggregateSchema = "STRUCT<A INTEGER, B INTEGER>",
      returnSchema = "STRUCT<A INTEGER, B INTEGER>")
  public static Udaf<Struct, Struct, Struct> createStructUdaf() {
    return new Udaf<Struct, Struct, Struct>() {

      @Override
      public Struct initialize() {
        return new Struct(SchemaBuilder.struct()
            .field("A", Schema.OPTIONAL_INT32_SCHEMA)
            .field("B", Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
            .put("A", 0)
            .put("B", 0);
      }

      @Override
      public Struct aggregate(final Struct current, final Struct aggregate) {
        aggregate.put("A", current.getInt32("A") + aggregate.getInt32("A"));
        aggregate.put("B", current.getInt32("B") + aggregate.getInt32("B"));
        return aggregate;
      }

      @Override
      public Struct merge(final Struct aggOne, final Struct aggTwo) {
        return aggregate(aggOne, aggTwo);
      }

      @Override
      public Struct map(final Struct agg) {
        return agg;
      }
    };
  }

  static class SumIntUdaf implements TableUdaf<Integer, Long, Long>, Configurable {

    public static final String INIT_CONFIG = "ksql.functions.test_udaf.init";
    private long init = 0L;

    @Override
    public Long undo(final Integer valueToUndo, final Long aggregateValue) {
      return aggregateValue - valueToUndo;
    }

    @Override
    public Long initialize() {
      return init;
    }

    @Override
    public Long aggregate(final Integer current, final Long aggregate) {
      return current + aggregate;
    }

    @Override
    public Long merge(final Long aggOne, final Long aggTwo) {
      return aggOne + aggTwo;
    }

    @Override
    public Long map(final Long agg) {
      return agg;
    }

    @Override
    public void configure(final Map<String, ?> map) {
      final Object init = map.get(INIT_CONFIG);
      this.init = (init == null) ? this.init : (long) init;
    }
  }
}
