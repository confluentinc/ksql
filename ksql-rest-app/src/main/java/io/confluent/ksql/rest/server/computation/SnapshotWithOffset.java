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

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.KsqlExecutionContext;

public class SnapshotWithOffset {
  private KsqlExecutionContext ksqlExecutionContext;
  private int snapshotOffset;

  public SnapshotWithOffset(final KsqlExecutionContext ksqlExecutionContext) {
    this.ksqlExecutionContext = ksqlExecutionContext;
    this.snapshotOffset = 0;
  }

  public KsqlExecutionContext getKsqlExecutionContext() {
    return ksqlExecutionContext.createSandbox(ksqlExecutionContext.getServiceContext());
  }

  public int getSnapshotOffset() {
    return snapshotOffset;
  }

  public void updateSnapshot(final KsqlExecutionContext ksqlExecutionContext) {
    this.ksqlExecutionContext = ksqlExecutionContext;
  }

  public void updateOffsetValue(final int snapshotOffset) {
    this.snapshotOffset = snapshotOffset;
  }
}

