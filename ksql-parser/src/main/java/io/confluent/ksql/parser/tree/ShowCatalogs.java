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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Optional;

public final class ShowCatalogs
    extends Statement {

  private final Optional<String> likePattern;

  public ShowCatalogs(final Optional<String> likePattern) {
    this(Optional.empty(), likePattern);
  }

  public ShowCatalogs(final NodeLocation location, final Optional<String> likePattern) {
    this(Optional.of(location), likePattern);
  }

  public ShowCatalogs(final Optional<NodeLocation> location, final Optional<String> likePattern) {
    super(location);
    this.likePattern = requireNonNull(likePattern, "likePattern is null");
  }

  public Optional<String> getLikePattern() {
    return likePattern;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitShowCatalogs(this, context);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    return (obj != null) && (getClass() == obj.getClass());
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
