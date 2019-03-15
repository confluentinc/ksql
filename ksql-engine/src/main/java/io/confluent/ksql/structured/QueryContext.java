/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.structured;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.query.QueryId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class QueryContext {
  private final QueryId queryId;
  private final List<String> context;

  private QueryContext(final QueryId queryId) {
    this(queryId, Collections.emptyList());
  }

  private QueryContext(final QueryId queryId, final List<String> context) {
    this.queryId = Objects.requireNonNull(queryId);
    this.context = Objects.requireNonNull(context);
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public List<String> getContext() {
    return context;
  }

  private QueryContext push(final String ...context) {
    return new QueryContext(
        queryId,
        new ImmutableList.Builder<String>()
            .addAll(this.context)
            .addAll(Arrays.asList(context))
            .build()
    );
  }

  public static class Stacker {
    final QueryContext queryContext;

    public Stacker(final QueryId queryId) {
      this.queryContext = Objects.requireNonNull(new QueryContext(queryId));
    }

    private Stacker(final QueryContext queryContext) {
      this.queryContext = Objects.requireNonNull(queryContext);
    }

    public Stacker push(final String ...context) {
      return new Stacker(queryContext.push(context));
    }

    public QueryContext getQueryContext() {
      return queryContext;
    }

    @Override
    public boolean equals(final Object o) {
      return o instanceof Stacker
          && Objects.equals(queryContext, ((Stacker)o).queryContext);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queryContext);
    }
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof QueryContext
        && Objects.equals(queryId ,((QueryContext)o).queryId)
        && Objects.equals(context, ((QueryContext)o).context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(context);
  }
}
