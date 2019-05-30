/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.ksql;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Set;

/**
 * Utility class for dealing with SQL identifiers in KSQL.
 */
public final class Identifiers {

  private Identifiers() {
  }

  private static final ImmutableSet<String> IMPLICIT_COLUMN_NAMES = ImmutableSet.of(
      SchemaUtil.ROWTIME_NAME, SchemaUtil.ROWKEY_NAME
  );

  public static boolean isImplicitColumnName(final String columnName) {
    return IMPLICIT_COLUMN_NAMES.contains(columnName.toUpperCase());
  }

  public static Set<String> implicitColumnNames() {
    return IMPLICIT_COLUMN_NAMES;
  }
}
