/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class RunScript extends Statement {

  public RunScript(final Optional<NodeLocation> location) {
    super(location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }

    return obj != null && obj.getClass().equals(getClass());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .toString();
  }
}
