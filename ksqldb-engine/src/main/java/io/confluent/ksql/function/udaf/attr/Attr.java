/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.attr;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
    name = "ATTR",
    description = "The ATTR() aggregation indicates there are multiple values, but only one "
        + "was expected. For example, if aggregating against a table that semantically should "
        + "have only one value for a column given a key, this aggregation enables users to "
        + "indicate that they expect only a single value.\n\n"
        + "If the aggregation encounters more than a single value for the expected singular "
        + "column, the entire aggregation will return null."
)
public final class Attr {

  private Attr() {
    // checkstyle complains otherwise
  }

  @UdafFactory(description = "Collect as a singleton")
  public static <T> TableUdaf<T, Struct, T> createAttr() {
    return new Impl<T>();
  }

  /**
   * This class implements the logic for checking that we only ever see a valid value. Note
   * that there is some complexity in this class to differentiate the case from a "valid null",
   * which is the case for when the aggregate is initialized, from an "invalid null".
   */
  @VisibleForTesting
  static class Impl<T> implements TableUdaf<T, Struct, T> {

    static final String INIT = "INIT";
    static final String VALID = "VALID";
    static final String VALUE = "VALUE";

    SqlType inType;
    Schema schema;

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Override
    public void initializeTypeArguments(final List<SqlArgument> args) {
      this.inType = args.get(0).getSqlTypeOrThrow();
      this.schema = SchemaConverters.sqlToConnectConverter()
          .toConnectSchema(getAggregateSqlType().get());
    }

    @Override
    public Optional<SqlType> getAggregateSqlType() {
      return Optional.of(
          SqlTypes.struct()
              .field(INIT, SqlTypes.BOOLEAN)
              .field(VALID, SqlTypes.BOOLEAN)
              .field(VALUE, inType)
              .build()
      );
    }

    @Override
    public Optional<SqlType> getReturnSqlType() {
      return Optional.of(inType);
    }

    @Override
    public Struct undo(final T valueToUndo, final Struct aggregateValue) {
      return aggregateValue;
    }

    @Override
    public Struct initialize() {
      return new Struct(schema)
          .put(INIT, false)
          .put(VALID, true)
          .put(VALUE, null);
    }

    @Override
    public Struct aggregate(final T current, final Struct aggregate) {
      if (!aggregate.getBoolean(VALID)) {
        return aggregate;
      }

      if (!aggregate.getBoolean(INIT)) {
        return aggregate
            .put(INIT, true)
            .put(VALID, true)
            .put(VALUE, current);
      }

      final Object old = aggregate.get(VALUE);
      return Objects.equals(old, current)
          ? aggregate
          : aggregate.put(VALID, false);

    }

    @Override
    public Struct merge(final Struct aggOne, final Struct aggTwo) {
      if (aggOne.getBoolean(INIT) && !aggTwo.getBoolean(INIT)) {
        return aggOne;
      } else if (!aggOne.getBoolean(INIT) && aggTwo.getBoolean(INIT)) {
        return aggTwo;
      } else if (!aggOne.getBoolean(VALID)) {
        return aggOne;
      } else if (!aggTwo.getBoolean(VALID)) {
        return aggTwo;
      }

      return aggOne.equals(aggTwo) ? aggOne : aggOne.put(INIT, false);

    }

    @SuppressWarnings("unchecked")
    @Override
    public T map(final Struct agg) {
      return agg.getBoolean(VALID) ? (T) agg.get(VALUE) : null;
    }
  }
}
