/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;


public class WindowName extends Node {

  private String windowName;

  public WindowName(final String windowName) {
    this(Optional.empty(), windowName);
  }

  public WindowName(final NodeLocation location, final String windowName) {
    this(Optional.of(location), windowName);
  }

  private WindowName(final Optional<NodeLocation> location, final String windowName) {
    super(location);
    this.windowName = windowName;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final WindowName o = (WindowName) obj;
    return Objects.equals(windowName, o.windowName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(windowName);
  }

  @Override
  public String toString() {
    return windowName;
  }
}
