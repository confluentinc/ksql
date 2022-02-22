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
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public class EarliestByOffsetFactory extends AggregateFunctionFactory {
  static final String FUNCTION_NAME = "EARLIEST_BY_OFFSET";

  public EarliestByOffsetFactory() {
    super(FUNCTION_NAME);
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

    if (initArgs != null) {
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
      } else if (args.size() > 2) {
        // JNH: This needs to be updated.
        // udaf.json specifies the error a bit too much.
        throw new KsqlException("Function 'EARLIEST_BY_OFFSET' does not accept "
            + "parameters (INTEGER, BOOLEAN, INTEGER).\nValid alternatives are:");
      }
    }

    final SqlType returnType = argTypeList.get(0).getSqlTypeOrThrow();
    final Schema connectType =
        SchemaConverters.sqlToConnectConverter().toConnectSchema(returnType);
    final Schema internalStructSchema = KudafByOffsetUtils.buildSchema(connectType);
    final SqlType aggregateType =
        SchemaConverters.connectToSqlConverter().toSqlType(internalStructSchema);
    if (n == 1) {
      return new EarliestByOffsetKudaf(initArgs, aggregateType, returnType, ignoreNulls,
          internalStructSchema);
    } else if (n > 1) {
      return new EarliestNByOffsetKudaf(
          initArgs,
          SqlArray.of(aggregateType),
          SqlArray.of(returnType),
          ignoreNulls,
          n,
          internalStructSchema);
    } else {
      throw new IllegalArgumentException("For EarliestByOffset, n must be positive.  It was " + n);
    }
  }

  @Override
  public List<List<ParamType>> supportedArgs() {
    // anything is a supported type
    return ImmutableList.of(ImmutableList.of());
  }
}
