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

package io.confluent.ksql.topic;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public enum Inject {
  SOURCE(Type.SOURCE, 1, (short) 1),
  SOURCE2(Type.SOURCE, 12, (short) 12),

  WITH(Type.WITH, 2, (short) 2),
  OVERRIDES(Type.OVERRIDES, 3, (short) 3),
  KSQL_CONFIG(Type.KSQL_CONFIG, 4, (short) 4),

  WITH_P(Type.WITH, 5, null),
  OVERRIDES_P(Type.OVERRIDES, 6, null),
  KSQL_CONFIG_P(Type.KSQL_CONFIG, 7, null),

  WITH_R(Type.WITH, null, (short) 8),
  OVERRIDES_R(Type.OVERRIDES, null, (short) 9),
  KSQL_CONFIG_R(Type.KSQL_CONFIG, null, (short) 10),

  NO_WITH(Type.WITH, null, null),
  NO_OVERRIDES(Type.OVERRIDES, null, null),
  NO_CONFIG(Type.KSQL_CONFIG, null, null)
  ;

  final Type type;
  final Integer partitions;
  final Short replicas;

  Inject(final Type type, final Integer partitions, final Short replicas) {
    this.type = type;
    this.partitions = partitions;
    this.replicas = replicas;
  }

  enum Type {
    WITH,
    OVERRIDES,
    KSQL_CONFIG,
    SOURCE
  }

  /**
   * Generates code for all combinations of Injects
   */
  public static void main(String[] args) {
    final List<Inject> withs = EnumSet.allOf(Inject.class)
        .stream().filter(i -> i.type == Type.WITH).collect(Collectors.toList());
    final List<Inject> overrides = EnumSet.allOf(Inject.class)
        .stream().filter(i -> i.type == Type.OVERRIDES).collect(Collectors.toList());
    final List<Inject> ksqlConfigs = EnumSet.allOf(Inject.class)
        .stream().filter(i -> i.type == Type.KSQL_CONFIG).collect(Collectors.toList());

    for (List<Inject> injects : Lists.cartesianProduct(withs, overrides, ksqlConfigs)) {
      // sort by precedence order
      injects = new ArrayList<>(injects);
      injects.sort(Comparator.comparing(i -> i.type));

      final Inject expectedPartitions =
          injects.stream().filter(i -> i.partitions != null).findFirst().orElse(Inject.SOURCE);
      final Inject expectedReplicas =
          injects.stream().filter(i -> i.replicas != null).findFirst().orElse(Inject.SOURCE);

      System.out.println(String.format("{new Inject[]{%-38s}, %-15s, %-15s},",
          injects.stream().map(Objects::toString).collect(Collectors.joining(", ")),
          expectedPartitions,
          expectedReplicas)
      );
    }
  }
}
