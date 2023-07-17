/*
 * Copyright 2022 Confluent Inc.
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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
  public static <T> TableUdaf<T, List<Struct>, T> createAttr() {
    return new Impl<T>();
  }

  @VisibleForTesting
  static class Impl<T> implements TableUdaf<T, List<Struct>, T> {

    static final String VALUE = "VALUE";
    static final String COUNT = "COUNT";

    SqlType inType;
    Schema entrySchema;

    @Override
    public void initializeTypeArguments(final List<SqlArgument> args) {
      this.inType = args.get(0).getSqlTypeOrThrow();

      // we use a list of structs instead of a map here for two reasons:
      //
      //   1. our data formats currently only support maps with string keys
      //   2. null is a valid entry, and most maps don't support null keys
      //
      // this should be OK from a complexity perspective because ATTR expects
      // a unique entry, so only the situations where it is used improperly will
      // take a runtime hit
      this.entrySchema = SchemaBuilder.struct()
          .optional()
          .field(VALUE, SchemaConverters.sqlToConnectConverter().toConnectSchema(inType))
          .field(COUNT, Schema.OPTIONAL_INT32_SCHEMA)
          .build();
    }

    @Override
    public Optional<SqlType> getAggregateSqlType() {
      return Optional.of(SqlTypes.array(
          SchemaConverters.connectToSqlConverter().toSqlType(entrySchema)
      ));
    }

    @Override
    public Optional<SqlType> getReturnSqlType() {
      return Optional.of(inType);
    }

    @Override
    public List<Struct> initialize() {
      return new ArrayList<>();
    }

    @Override
    public List<Struct> aggregate(final T current, final List<Struct> agg) {
      final List<Struct> out = new ArrayList<>(agg);
      update(out, current, 1);
      return out;
    }

    @Override
    public List<Struct> merge(final List<Struct> one, final List<Struct> two) {
      // use O(n^2) algorithm here because in practice each of these lists
      // should have no more than one entry (otherwise it's an invalid Attr
      // anyway)
      final List<Struct> out = new ArrayList<>(one);
      for (final Struct entry : two) {
        update(out, entry.get(VALUE), entry.getInt32(COUNT));
      }
      return out;
    }

    @Override
    public List<Struct> undo(final T valueToUndo, final List<Struct> agg) {
      final List<Struct> out = new ArrayList<>(agg);
      update(out, valueToUndo, -1);
      return out;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T map(final List<Struct> agg) {
      final List<Struct> collect = agg.stream()
          .filter(s -> s.getInt32(COUNT) > 0)
          .collect(Collectors.toList());

      if (collect.size() != 1) {
        return null;
      }

      return (T) collect.get(0).get(VALUE);
    }

    private void update(final List<Struct> agg, final Object current, final int count) {
      boolean found = false;
      for (final Struct entry : agg) {
        if (Objects.equals(entry.get(VALUE), current)) {
          found = true;
          entry.put(COUNT, Math.max(0, entry.getInt32(COUNT) + count));
          break;
        }
      }

      if (!found && count > 0) {
        agg.add(new Struct(entrySchema).put(VALUE, current).put(COUNT, count));
      }
    }
  }
}
