/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlConstants;

@SuppressWarnings("MethodMayBeStatic")
@UdfDescription(
    name = AsValue.NAME_TEXT,
    description = AsValue.DESCRIPTION,
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class AsValue {

  static final String NAME_TEXT = "AS_VALUE";

  public static final FunctionName NAME = FunctionName.of(NAME_TEXT);


  static final String DESCRIPTION = "Used exclusively in the projection of a query to allow a key"
      + "column to be copied into the value schema. Has no affect if used anywhere else.";


  @Udf
  public <T> T asValue(final T keyColumn) {
    return keyColumn;
  }
}

