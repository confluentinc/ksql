/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.util;

import org.apache.commons.lang3.StringUtils;

public final class WithClauseUtil {

  private WithClauseUtil() { }

  public static int parsePartitions(final String expression) {
    try {
      final int partitions = Integer.parseInt(StringUtils.strip(expression, "'"));
      if (partitions <= 0) {
        throw new KsqlException("Invalid number of partitions in WITH clause (must be positive): "
            + partitions);
      }
      return partitions;
    } catch (NumberFormatException e) {
      throw new KsqlException("Invalid number of partitions in WITH clause: " + expression, e);
    }
  }

  public static short parseReplicas(final String expression) {
    try {
      final short replicas = Short.parseShort(StringUtils.strip(expression, "'"));
      if (replicas <= 0) {
        throw new KsqlException("Invalid number of replicas in WITH clause (must be positive): "
            + replicas);
      }
      return replicas;
    } catch (NumberFormatException e) {
      throw new KsqlException("Invalid number of replicas in WITH clause: " + expression, e);
    }
  }

}
