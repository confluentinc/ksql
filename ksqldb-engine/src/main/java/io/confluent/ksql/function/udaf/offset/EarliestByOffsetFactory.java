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

package io.confluent.ksql.function.udaf.offset;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.UdafAggregateFunctionFactory;
import io.confluent.ksql.function.UdfIndex;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public class EarliestByOffsetFactory extends AggregateFunctionFactory {
  static final String NAME = "EARLIEST_BY_OFFSET";

  public EarliestByOffsetFactory() {
    super(NAME);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public KsqlAggregateFunction createAggregateFunction(
      final List<SqlArgument> argTypeList,
      final AggregateFunctionInitArguments initArgs
  ) {
    // This parses the various initArgs that could be used.
    // If there are two arguments, then they should be (Boolean, Integer)
    // A single parameter could be passed in to handle either setting "n" or "ignoreNulls".
    boolean ignoreNulls = true;
    int n = 1;

    if (initArgs != null && !initArgs.args().isEmpty()) {
      final List<Object> args = initArgs.args();
      if (args.size() == 2 && args.get(0) instanceof Integer && args.get(1) instanceof Boolean) {
        n = (Integer)args.get(0);
        ignoreNulls = (Boolean) args.get(1);
      } else if (args.size() == 1
          && (args.get(0) instanceof Boolean || args.get(0) instanceof Integer)) {
        final Object arg = args.get(0);
        if (arg instanceof Boolean) {
          ignoreNulls = (Boolean) arg;
        } else if (arg instanceof Integer) {
          n = (Integer) arg;
        }
      } else { // if (args.size() > 2) {
        final List<SqlArgument> inputArgs = UdafAggregateFunctionFactory.buildAllParams(argTypeList,
            initArgs, NAME);
        throw new KsqlException("Function 'EARLIEST_BY_OFFSET' does not accept "
            + "parameters " + UdfIndex.getParamsAsString(inputArgs) + ".\n" + usage);
      }
    }

    final SqlType returnType = argTypeList.get(0).getSqlTypeOrThrow();
    final Schema connectType =
        SchemaConverters.sqlToConnectConverter().toConnectSchema(returnType);
    final Schema internalStructSchema = KudafByOffsetUtils.buildSchema(connectType);
    final SqlType aggregateType =
        SchemaConverters.connectToSqlConverter().toSqlType(internalStructSchema);
    if (n == 1) {
      return new EarliestByOffsetKudaf(NAME, initArgs, aggregateType, returnType,
          ignoreNulls, internalStructSchema);
    } else if (n > 1) {
      return new EarliestNByOffsetKudaf(NAME, initArgs, SqlArray.of(aggregateType),
          SqlArray.of(returnType), ignoreNulls, n, internalStructSchema);
    } else {
      throw new IllegalArgumentException(
          "For EarliestByOffset, n must be positive.  It was " + n + ".");
    }
  }

  @Override
  public List<List<ParamType>> supportedArgs() {
    return ImmutableList.of(ImmutableList.of());
  }

  private static final String usage = "Valid alternatives are:\nEARLIEST_BY_OFFSET(T val)\n"
      + "EARLIEST_BY_OFFSET(T val, BOOLEAN ignoreNulls)\n"
      + "EARLIEST_BY_OFFSET(T val, INT earliestN)\n"
      + "EARLIEST_BY_OFFSET(T val, INT earliestN, BOOLEAN ignoreNulls)";
}
