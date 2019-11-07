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

package io.confluent.ksql.execution.context;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class QueryContext {

  private final List<String> context;

  private QueryContext() {
    this(Collections.emptyList());
  }

  private QueryContext(List<String> context) {
    this.context = Objects.requireNonNull(context);
  }

  public List<String> getContext() {
    return context;
  }

  private QueryContext push(String... context) {
    return new QueryContext(
        new ImmutableList.Builder<String>()
            .addAll(this.context)
            .addAll(Arrays.asList(context))
            .build()
    );
  }

  public static class Stacker {

    final QueryContext queryContext;

    public Stacker() {
      this.queryContext = new QueryContext();
    }

    public static Stacker of(QueryContext queryContext) {
      return new Stacker(queryContext);
    }

    private Stacker(QueryContext queryContext) {
      this.queryContext = Objects.requireNonNull(queryContext);
    }

    public Stacker push(String... context) {
      return new Stacker(queryContext.push(context));
    }

    public QueryContext getQueryContext() {
      return queryContext;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Stacker
          && Objects.equals(queryContext, ((Stacker) o).queryContext);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queryContext);
    }
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof QueryContext
        && Objects.equals(context, ((QueryContext) o).context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(context);
  }
}
