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
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import java.util.Objects;
import java.util.Optional;

/**
 * Pojo holding sink information
 */
@Immutable
public final class Sink {

  private final String name;
  private final boolean createSink;
  private final CreateSourceAsProperties properties;
  private final Optional<String> partitionBy;

  /**
   * Info about the sink of a query.
   *
   * @param name the name of the sink.
   * @param createSink indicates if name should be created, (CSAS/CTAS), or not (INSERT INTO).
   * @param properties properties of the sink.
   * @param partitionBy an optional partition by expression
   * @return the pojo.
   */
  public static Sink of(
      final String name,
      final boolean createSink,
      final CreateSourceAsProperties properties,
      final Optional<Expression> partitionBy
  ) {
    final Optional<String> partitionByExp = partitionBy
        .map(Object::toString)
        .map(String::toUpperCase);

    return new Sink(name, createSink, properties, partitionByExp);
  }

  private Sink(
      final String name,
      final boolean createSink,
      final CreateSourceAsProperties properties,
      final Optional<String> partitionBy
  ) {
    this.name = requireNonNull(name, "name");
    this.properties = requireNonNull(properties, "properties");
    this.createSink = createSink;
    this.partitionBy = Objects.requireNonNull(partitionBy, "partitionBy");
  }

  public String getName() {
    return name;
  }

  public boolean shouldCreateSink() {
    return createSink;
  }

  public CreateSourceAsProperties getProperties() {
    return properties;
  }

  public Optional<String> getPartitionBy() {
    return partitionBy;
  }
}
