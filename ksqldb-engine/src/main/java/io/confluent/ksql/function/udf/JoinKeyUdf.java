/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.function.udf;

import io.confluent.ksql.function.udf.nulls.Coalesce;
import io.confluent.ksql.name.FunctionName;

@SuppressWarnings("MethodMayBeStatic")
@UdfDescription(name = JoinKeyUdf.NAME_TEXT, description = JoinKeyUdf.DESCRIPTION)
public class JoinKeyUdf {

  private static final Coalesce impl = new Coalesce();

  static final String NAME_TEXT = "JOINKEY";

  public static final FunctionName NAME = FunctionName.of(NAME_TEXT);

  static final String DESCRIPTION = "Used to express the synthetic join key created by some joins "
      + "withing the projection of the query."
      + "\nOUTER joins or joins where no join criteria is a simple column expression generate a "
      + "synthetic key column. This column must be included in the projection of persistent "
      + "queries. " + NAME_TEXT + " can be used within the projection to represent this synthetic "
      + "key column. For example: "
      + "\n\tSELECT JOINKEY(ABS(L.ID), ABS(R.ID) AS ID, L.NAME, R.PRICE "
      + "FROM L JOIN R ON ABS(L.ID) = ABS(R.ID);"
      + "\nIf used outside of such joins the function returns the same as the "
      + Coalesce.NAME_TEXT + " function.";

  @Udf
  public <T> T joinKey(final T left, final T right) {
    return impl.coalesce(left, right);
  }
}
