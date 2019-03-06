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

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.concurrent.Immutable;

/**
 * Pojo holding sink information
 */
@Immutable
public final class Sink {

  private final Table sink;
  private final boolean createSink;
  private final Map<String, Expression> properties;

  /**
   * Info about the target of the query.
   *
   * @param sink the sink table/stream the query will output to.
   * @param createSink indicates if sink should be created, (CSAS/CTAS), or not (INSERT INTO).
   * @param properties properties of the sink.
   * @return the pojo.
   */
  public static Sink of(
      final Table sink,
      final boolean createSink,
      final Map<String, Expression> properties
  ) {
    return new Sink(sink, createSink, properties);
  }

  private Sink(
      final Table sink,
      final boolean createSink,
      final Map<String, Expression> properties
  ) {
    this.sink = requireNonNull(sink, "sink");
    this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties"));
    this.createSink = createSink;
  }

  public Table getSink() {
    return sink;
  }

  public boolean shouldCreateSink() {
    return createSink;
  }

  public Map<String, Expression> getProperties() {
    return properties;
  }
}
