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

package io.confluent.ksql.test.model;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.model.SemanticVersion;
import java.util.Objects;

@Immutable
public final class KsqlVersion {

  private final String name;
  private final SemanticVersion version;

  public static KsqlVersion of(final String name, final SemanticVersion version) {
    return new KsqlVersion(name, version);
  }

  public String getName() {
    return name;
  }

  public SemanticVersion getVersion() {
    return version;
  }

  private KsqlVersion(final String name, final SemanticVersion version) {
    this.name = Objects.requireNonNull(name, "name");
    this.version = Objects.requireNonNull(version, "version");
  }

  @Override
  public String toString() {
    return name + " (" + version + ")";
  }
}
