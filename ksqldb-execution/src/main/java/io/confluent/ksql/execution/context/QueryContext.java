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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public final class QueryContext {
  private static final String DELIMITER = "/";

  private final ImmutableList<String> context;

  private QueryContext() {
    this(Collections.emptyList());
  }

  private QueryContext(final List<String> context) {
    this.context = ImmutableList.copyOf(Objects.requireNonNull(context));
    for (final String frame : context) {
      if (frame.contains(DELIMITER)) {
        throw new IllegalArgumentException("Cannot use string with delimiter in context");
      }
    }
  }

  @JsonCreator
  private QueryContext(final String context) {
    this(ImmutableList.copyOf(context.split(DELIMITER)));
  }

  @JsonIgnore
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "context is ImmutableList")
  public List<String> getContext() {
    return context;
  }

  @JsonValue
  public String formatContext() {
    return String.join(DELIMITER, context);
  }

  public String toString() {
    return formatContext();
  }

  private QueryContext push(final String ...context) {
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

    public static Stacker of(final QueryContext queryContext) {
      return new Stacker(queryContext);
    }

    private Stacker(final QueryContext queryContext) {
      this.queryContext = Objects.requireNonNull(queryContext);
    }

    public Stacker push(final String... context) {
      return new Stacker(queryContext.push(context));
    }

    public QueryContext getQueryContext() {
      return queryContext;
    }

    @Override
    public boolean equals(final Object o) {
      return o instanceof Stacker
          && Objects.equals(queryContext, ((Stacker) o).queryContext);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queryContext);
    }
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof QueryContext
        && Objects.equals(context, ((QueryContext) o).context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(context);
  }
}
