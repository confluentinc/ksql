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

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.util.KsqlHostInfo;


public final class RemoteSourceDescription {
  private final String sourceName;
  private final SourceDescription sourceDescription;
  private final KsqlHostInfo hostInfo;

  public RemoteSourceDescription(
      final String sourceName,
      final SourceDescription sourceDescription,
      final KsqlHostInfo hostInfo
  ) {
    this.sourceName = sourceName;
    this.sourceDescription = sourceDescription;
    this.hostInfo = hostInfo;
  }

  public String getSourceName() {
    return sourceName;
  }

  public SourceDescription getSourceDescription() {
    return sourceDescription;
  }

  public KsqlHostInfo getKsqlHostInfo() {
    return hostInfo;
  }
}
