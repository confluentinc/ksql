/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.tools.migrations.util;

import io.confluent.ksql.tools.migrations.MigrationException;
import java.util.Objects;

public class MigrationFile {
  private final int version;
  private final String name;
  private final String filepath;

  public MigrationFile(
      final int version,
      final String name,
      final String filepath
  ) {
    if (version < 1) {
      throw new MigrationException("Version must be positive, received " + version);
    }
    this.version = version;
    this.name = Objects.requireNonNull(name);
    this.filepath = Objects.requireNonNull(filepath);
  }

  public int getVersion() {
    return version;
  }

  public String getName() {
    return name;
  }

  public String getFilepath() {
    return filepath;
  }
}
