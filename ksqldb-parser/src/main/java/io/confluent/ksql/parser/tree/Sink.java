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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;

/**
 * Pojo holding sink information
 */
@Immutable
public final class Sink {

  private final SourceName name;
  private final CreateSourceAsProperties properties;
  private final boolean replaces;
  private final boolean createSink;

  /**
   * Info about the sink of a query.
   *
   * @param name the name of the sink.
   * @param createSink indicates if name should be created, (CSAS/CTAS), or not (INSERT INTO).
   * @param replaces indicates whether or not there is an existing query that populates this sync,
   *                 which will be replaced as part of a query upgrade
   * @param properties properties of the sink.
   * @return the pojo.
   */
  public static Sink of(
      final SourceName name,
      final boolean createSink,
      final boolean replaces,
      final CreateSourceAsProperties properties
  ) {
    return new Sink(name, createSink, replaces, properties);
  }

  private Sink(
      final SourceName name,
      final boolean createSink,
      final boolean replaces,
      final CreateSourceAsProperties properties) {
    this.name = requireNonNull(name, "name");
    this.properties = requireNonNull(properties, "properties");
    this.replaces = replaces;
    this.createSink = createSink;
  }

  public SourceName getName() {
    return name;
  }

  public boolean shouldCreateSink() {
    return createSink;
  }

  public boolean shouldReplace() {
    return replaces;
  }

  public CreateSourceAsProperties getProperties() {
    return properties;
  }
}
