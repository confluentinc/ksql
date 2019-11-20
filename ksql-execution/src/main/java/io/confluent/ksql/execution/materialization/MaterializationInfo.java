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

package io.confluent.ksql.execution.materialization;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.sqlpredicate.SqlPredicate;
import io.confluent.ksql.execution.transform.KsqlValueTransformerWithKey;
import io.confluent.ksql.execution.transform.SelectValueMapper;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.kstream.Predicate;

/**
 * Pojo for passing around information about materialization of a query's state store
 */
@Immutable
public final class MaterializationInfo {

  private final String stateStoreName;
  private final LogicalSchema stateStoreSchema;
  private final List<TransformInfo> transforms;
  private final LogicalSchema schema;

  public String stateStoreName() {
    return stateStoreName;
  }

  public LogicalSchema getStateStoreSchema() {
    return stateStoreSchema;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public List<TransformInfo> getTransforms() {
    return transforms;
  }

  private MaterializationInfo(
      String stateStoreName, LogicalSchema stateStoreSchema, List<TransformInfo> transforms,
      LogicalSchema schema
  ) {
    this.stateStoreName = requireNonNull(stateStoreName, "stateStoreName");
    this.stateStoreSchema = requireNonNull(stateStoreSchema, "stateStoreSchema");
    this.transforms = ImmutableList.copyOf(requireNonNull(transforms, "transforms"));
    this.schema = requireNonNull(schema, "schema");
  }

  /**
   * Create a MaterializationInfo builder.
   *
   * @param stateStoreName   the name of the state store
   * @param stateStoreSchema the schema of the data in the state store
   * @return builder instance.
   */
  public static Builder builder(String stateStoreName, LogicalSchema stateStoreSchema) {
    return new Builder(stateStoreName, stateStoreSchema);
  }

  public static final class Builder {

    private final String stateStoreName;
    private final LogicalSchema stateStoreSchema;
    private final List<TransformInfo> transforms;
    private LogicalSchema schema;

    private Builder(String stateStoreName, LogicalSchema stateStoreSchema) {
      this.stateStoreName = Objects.requireNonNull(stateStoreName, "stateStoreName");
      this.stateStoreSchema = Objects.requireNonNull(stateStoreSchema, "stateStoreSchema");
      this.transforms = new LinkedList<>();
      this.schema = stateStoreSchema;
    }

    /**
     * Adds an aggregate map transform for mapping final result of complex aggregates (e.g. avg)
     *
     * @param aggregator the aggregator to apply.
     * @param resultSchema schema after applying aggregate result mapping.
     * @return A builder instance with this transformation.
     */
    public Builder mapAggregates(
        final KudafAggregator aggregator,
        final LogicalSchema resultSchema
    ) {
      transforms.add(new AggregateMapInfo(aggregator));
      this.schema = Objects.requireNonNull(resultSchema, "resultSchema");
      return this;
    }

    /**
     * Adds a transform that projects a list of expressions from the value.
     *
     * @param selectMapper The select mapper to apply.
     * @param resultSchema The schema after applying the projection.
     * @return A builder instance with this transformation.
     */
    public <K> Builder project(
        final SelectValueMapper<K> selectMapper,
        final LogicalSchema resultSchema
    ) {
      transforms.add(new ProjectInfo(selectMapper));
      this.schema = Objects.requireNonNull(resultSchema, "resultSchema");
      return this;
    }

    /**
     * Adds a transform that filters rows from the materialization.
     *
     * @param predicate the predicate to apply.
     * @return A builder instance with this transformation.
     */
    public Builder filter(final SqlPredicate predicate) {
      transforms.add(new SqlPredicateInfo(predicate));
      return this;
    }

    /**
     * Builds a MaterializationInfo with the properties and transforms in the builder.
     *
     * @return a MaterializationInfo instance.
     */
    public MaterializationInfo build() {
      return new MaterializationInfo(stateStoreName, stateStoreSchema, transforms, schema);
    }
  }

  public interface TransformInfo {

    <R> R visit(TransformVisitor<R> visitor);
  }

  public interface TransformVisitor<R> {

    R visit(AggregateMapInfo mapInfo);

    R visit(SqlPredicateInfo info);

    R visit(ProjectInfo info);
  }

  public static class AggregateMapInfo implements TransformInfo {

    private final KudafAggregator aggregator;

    AggregateMapInfo(final KudafAggregator aggregator) {
      this.aggregator = Objects.requireNonNull(aggregator, "aggregator");
    }

    public KudafAggregator getAggregator() {
      return aggregator;
    }

    public <R> R visit(TransformVisitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  public static class SqlPredicateInfo implements TransformInfo {

    private final SqlPredicate predicate;

    SqlPredicateInfo(final SqlPredicate predicate) {
      this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    public Predicate<Object, GenericRow> getPredicate(
        final ProcessingLogger logger
    ) {
      return predicate.getPredicate(logger);
    }

    @Override
    public <R> R visit(TransformVisitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  public static class ProjectInfo implements TransformInfo {

    private final SelectValueMapper selectMapper;

    ProjectInfo(final SelectValueMapper selectMapper) {
      this.selectMapper = Objects.requireNonNull(selectMapper, "selectMapper");
    }

    @SuppressWarnings("unchecked")
    public KsqlValueTransformerWithKey<Object> getSelectTransformer(
        final ProcessingLogger logger
    ) {
      return selectMapper.getTransformer(logger);
    }

    @Override
    public <R> R visit(TransformVisitor<R> visitor) {
      return visitor.visit(this);
    }
  }
}

