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

package io.confluent.ksql.tools.migrations;

import io.confluent.ksql.tools.migrations.util.MetadataUtil;
import java.util.Objects;

public class Migration {
  private final String version;
  private final String name;
  private final String checksum;
  private final String previous;
  private final String command;

  public Migration(
      final int version,
      final String name,
      final String checksum,
      final String command
  ) {
    if (version < 1) {
      throw new MigrationException("Version must be positive, received " + version);
    }
    this.version = Integer.toString(version);
    this.name = Objects.requireNonNull(name);
    this.checksum = Objects.requireNonNull(checksum);
    this.previous = version == 1 ? MetadataUtil.NONE_VERSION : Integer.toString(version - 1);
    this.command = Objects.requireNonNull(command);
  }

  public String getVersion() {
    return version;
  }

  public String getChecksum() {
    return checksum;
  }

  public String getName() {
    return name;
  }

  public String getCommand() {
    return command;
  }

  public String getPrevious() {
    return previous;
  }
}
