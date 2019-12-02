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

package io.confluent.ksql.execution.streams.materialization;

import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.util.KsqlException;

public final class PullProcessingContext implements KsqlProcessingContext {

  public static final PullProcessingContext INSTANCE = new PullProcessingContext();

  private PullProcessingContext() {
  }

  @Override
  public long getRowTime() {
    throw new KsqlException("ROWTIME is not currently exposed by pull queries. "
        + "See https://github.com/confluentinc/ksql/issues/3623.");
  }
}
