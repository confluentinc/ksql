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

package io.confluent.support.metrics.common;

public enum CollectorType {
  BASIC(0, "basic"), FULL(1, "full");
  public final int id;
  public final String name;

  CollectorType(final int id, final String name) {
    this.id = id;
    this.name = name;
  }

  public static io.confluent.support.metrics.common.CollectorType forId(final int id) {
    switch (id) {
      case 0:
        return BASIC;
      case 1:
        return FULL;
      default:
        throw new IllegalArgumentException("Unknown collector type id: " + id);
    }
  }

  public static io.confluent.support.metrics.common.CollectorType forName(final String name) {
    if (BASIC.name.equals(name)) {
      return BASIC;
    } else if (FULL.name.equals(name)) {
      return FULL;
    } else {
      throw new IllegalArgumentException("Unknown collector name: " + name);
    }
  }
}
