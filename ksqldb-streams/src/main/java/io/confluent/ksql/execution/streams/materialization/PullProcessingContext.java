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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import java.util.Objects;

@Immutable
public final class PullProcessingContext implements KsqlProcessingContext {

  private final long rowTime;

  public PullProcessingContext(final long rowTime) {
    this.rowTime = rowTime;
  }

  @Override
  public long getRowTime() {
    return rowTime;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PullProcessingContext that = (PullProcessingContext) o;
    return rowTime == that.rowTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowTime);
  }
}
