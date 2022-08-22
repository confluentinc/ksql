/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@UdafDescription(
        name = "OBJ_COL_ARG",
        description = "Returns an array of rows where all the given columns are non-null.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class ObjVarColArgUdaf
        implements Udaf<Pair<Integer, VariadicArgs<Object>>, List<Integer>, List<Integer>> {

  @UdafFactory(description = "Testing factory")
  public static Udaf<Pair<Integer, VariadicArgs<Object>>, List<Integer>, List<Integer>>
      createObjVarArgUdaf() {
    return new ObjVarColArgUdaf();
  }

  private SqlType firstType;

  @Override
  public List<Integer> initialize() {
    return new ArrayList<>();
  }

  @Override
  public List<Integer> aggregate(Pair<Integer, VariadicArgs<Object>> current,
                                 List<Integer> aggregate) {
    final boolean isLeftNonNull = current.getLeft() != null;
    final boolean isRightNonNull = current.getRight().stream().allMatch(Objects::nonNull);

    if (isLeftNonNull && isRightNonNull) {
      aggregate.add(current.getLeft());
    }

    return aggregate;
  }

  @Override
  public List<Integer> merge(List<Integer> aggOne, List<Integer> aggTwo) {
    aggOne.addAll(aggTwo);
    return aggOne;
  }

  @Override
  public List<Integer> map(List<Integer> agg) {
    return agg;
  }

  @Override
  public void initializeTypeArguments(List<SqlArgument> argTypeList) {
    firstType = argTypeList.get(0).getSqlTypeOrThrow();
  }

  @Override
  public Optional<SqlType> getAggregateSqlType() {
    return Optional.of(SqlArray.of(firstType));
  }

  @Override
  public Optional<SqlType> getReturnSqlType() {
    return Optional.of(SqlArray.of(firstType));
  }
}
