/*
 * Copyright 2022 Confluent Inc.
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
    name = "bad_test_udaf",
    description = "bad_test_udaf",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public final class BadTestUdaf {

  private BadTestUdaf() {
  }

  @SuppressFBWarnings("DM_EXIT")
  private static void runBadCode() {
    System.exit(-1);
  }

  @UdafFactory(description = "sums longs with bad 'undo' method")
  public static TableUdaf<Long, Long, Long> createSumLong() {
    return new TableUdaf<Long, Long, Long>() {
      @Override
      public Long undo(final Long valueToUndo, final Long aggregateValue) {
        runBadCode();
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

  @UdafFactory(description = "sums int with a bad factory call")
  public static TableUdaf<List<Integer>, Long, Long> createFactoryExiting() {
    runBadCode();
    return null;
  }

  @UdafFactory(description = "sums int")
  public static TableUdaf<Integer, Long, Long> createSumInt() {
    return new SumIntUdaf();
  }

  @UdafFactory(description = "sums double with a bad initialize")
  public static Udaf<Double, Double, Double> createSumDouble() {
    return new Udaf<Double, Double, Double>() {
      @Override
      public Double initialize() {
        runBadCode();
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

  @UdafFactory(description = "sums the length of strings with a bad aggregate")
  public static Udaf<String, Long, Long> createSumLengthString() {
    return new Udaf<String, Long, Long>() {
      @Override
      public Long initialize() {
        return (long) "initial".length();
      }

      @Override
      public Long aggregate(final String s, final Long aggregate) {
        runBadCode();
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
      description = "returns a struct with {SUM(in->A), SUM(in->B)} with a bad merger",
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
        runBadCode();
        return aggregate(aggOne, aggTwo);
      }

      @Override
      public Struct map(final Struct agg) {
        return agg;
      }
    };
  }

  // With a bad map method
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
      runBadCode();
      return agg;
    }

    @Override
    public void configure(final Map<String, ?> map) {
      runBadCode();
      final Object init = map.get(INIT_CONFIG);
      this.init = (init == null) ? this.init : (long) init;
    }
  }

  @UdafFactory(
      description = "bad map",
      paramSchema = "BOOLEAN",
      aggregateSchema = "BOOLEAN",
      returnSchema = "BOOLEAN")
  public static Udaf<Boolean, Boolean, Boolean> createBadMapUdaf() {
    return new Udaf<Boolean, Boolean, Boolean>() {

      @Override
      public Boolean initialize() {
        return Boolean.FALSE;
      }

      @Override
      public Boolean aggregate(Boolean current, Boolean aggregate) {
        return Boolean.FALSE;
      }

      @Override
      public Boolean merge(Boolean aggOne, Boolean aggTwo) {
        return Boolean.FALSE;
      }

      @Override
      public Boolean map(Boolean agg) {
        runBadCode();
        return Boolean.FALSE;
      }
    };
  }

  @UdafFactory(description = "sums the length of strings with a bad aggregate")
  public static Udaf<List<Long>, Long, Long> createBadAggregateTypeUdaf() {
    return new Udaf<List<Long>, Long, Long>() {
      @Override
      public Long initialize() {
        return null;
      }

      @Override
      public Long aggregate(List<Long> current, Long aggregate) {
        return null;
      }

      @Override
      public Long merge(Long aggOne, Long aggTwo) {
        return null;
      }

      @Override
      public Long map(Long agg) {
        return null;
      }

      @Override
      public Optional<SqlType> getAggregateSqlType() {
        runBadCode();
        return Optional.empty();
      }
    };
  }

  @UdafFactory(description = "sums the length of strings with a bad aggregate")
  public static Udaf<List<Boolean>, Long, Long> createBadReturnTypeUdaf() {
    return new Udaf<List<Boolean>, Long, Long>() {
      @Override
      public Long initialize() {
        return null;
      }

      @Override
      public Long aggregate(List<Boolean> current, Long aggregate) {
        return null;
      }

      @Override
      public Long merge(Long aggOne, Long aggTwo) {
        return null;
      }

      @Override
      public Long map(Long agg) {
        return null;
      }

      @Override
      public Optional<SqlType> getReturnSqlType() {
        runBadCode();
        return Optional.empty();
      }
    };
  }

}
