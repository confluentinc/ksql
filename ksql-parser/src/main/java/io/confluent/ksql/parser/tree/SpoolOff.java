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

package io.confluent.ksql.parser.tree;

import com.google.errorprone.annotations.Immutable;
import java.util.Optional;

@Immutable
public class SpoolOff extends Statement {

  public SpoolOff(final Optional<NodeLocation> location) {
    super(location);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public boolean equals(final Object obj) {
    return obj instanceof SpoolOff;
  }

  @Override
  public String toString() {
    return "Spool{OFF}";
  }
}
