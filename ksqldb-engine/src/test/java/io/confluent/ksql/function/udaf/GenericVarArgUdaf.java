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
import io.confluent.ksql.util.Triple;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@UdafDescription(
        name = "GENERIC_VAR_ARG",
        description = "Returns an array of rows where all the given columns are non-null.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class GenericVarArgUdaf<A, B, C>
        implements Udaf<Triple<A, B, VariadicArgs<C>>, List<A>, List<A>> {

  @UdafFactory(description = "Testing factory")
  public static <A, B, C> Udaf<Triple<A, B, VariadicArgs<C>>, List<A>, List<A>>
      createGenericVarArgUdaf() {
    return new GenericVarArgUdaf<>();
  }

  private SqlType firstType;

  @Override
  public List<A> initialize() {
    return new ArrayList<>();
  }

  @Override
  public List<A> aggregate(Triple<A, B, VariadicArgs<C>> current, List<A> aggregate) {
    final boolean isLeftNonNull = current.getLeft() != null;
    final boolean isMidNonNull = current.getMiddle() != null;
    final boolean isRightNonNull = current.getRight().stream().allMatch(Objects::nonNull);

    if (isLeftNonNull && isMidNonNull && isRightNonNull) {
      aggregate.add(current.getLeft());
    }

    return aggregate;
  }

  @Override
  public List<A> merge(List<A> aggOne, List<A> aggTwo) {
    aggOne.addAll(aggTwo);
    return aggOne;
  }

  @Override
  public List<A> map(List<A> agg) {
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
