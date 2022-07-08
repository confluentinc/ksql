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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Pojo for passing around information about materialization of a query's state store
 */
@Immutable
public final class MaterializationInfo {

  private final String stateStoreName;
  private final LogicalSchema stateStoreSchema;
  private final ImmutableList<TransformInfo> transforms;
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

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "transforms is ImmutableList")
  public List<TransformInfo> getTransforms() {
    return transforms;
  }

  private MaterializationInfo(
      final String stateStoreName,
      final LogicalSchema stateStoreSchema,
      final List<TransformInfo> transforms,
      final LogicalSchema schema
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
  public static Builder builder(final String stateStoreName, final LogicalSchema stateStoreSchema) {
    return new Builder(stateStoreName, stateStoreSchema);
  }

  public static final class Builder {

    private final String stateStoreName;
    private final LogicalSchema stateStoreSchema;
    private final List<TransformInfo> transforms;
    private LogicalSchema schema;

    private Builder(final String stateStoreName, final LogicalSchema stateStoreSchema) {
      this.stateStoreName = requireNonNull(stateStoreName, "stateStoreName");
      this.stateStoreSchema = requireNonNull(stateStoreSchema, "stateStoreSchema");
      this.transforms = new LinkedList<>();
      this.schema = stateStoreSchema;
    }

    /**
     * Adds a transform that maps the (key, value) to a new value.
     *
     * @param mapperFactory a factory from which to get the mapper to apply.
     * @param resultSchema schema after applying aggregate result mapping.
     * @param queryContext the query context of the step, used to create the processing logger
     * @return A builder instance with this transformation.
     */
    public Builder map(
        final TransformFactory<KsqlTransformer<Object, GenericRow>> mapperFactory,
        final LogicalSchema resultSchema,
        final QueryContext queryContext
    ) {
      transforms.add(new MapperInfo(mapperFactory, queryContext));
      this.schema = Objects.requireNonNull(resultSchema, "resultSchema");
      return this;
    }

    /**
     * Adds a transform that filters rows from the materialization.
     *
     * @param predicateFactory a factory from which to get the predicate to apply.
     * @param queryContext the query context of the step, used to create the processing logger
     * @return A builder instance with this transformation.
     */
    public Builder filter(
        final TransformFactory<KsqlTransformer<Object, Optional<GenericRow>>> predicateFactory,
        final QueryContext queryContext
    ) {
      transforms.add(new PredicateInfo(predicateFactory, queryContext));
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

  @EffectivelyImmutable
  public interface TransformInfo {

    <R> R visit(TransformVisitor<R> visitor);
  }

  public interface TransformVisitor<R> {

    R visit(MapperInfo mapInfo);

    R visit(PredicateInfo info);
  }

  public interface TransformFactory<T> {

    T apply(ProcessingLogger processingLogger);
  }

  public static class MapperInfo implements TransformInfo {

    private final TransformFactory<KsqlTransformer<Object, GenericRow>> mapperFactory;
    private final QueryContext queryContext;

    MapperInfo(
        final TransformFactory<KsqlTransformer<Object, GenericRow>> mapperFactory,
        final QueryContext queryContext
    ) {
      this.mapperFactory = requireNonNull(mapperFactory, "mapperFactory");
      this.queryContext = requireNonNull(queryContext, "queryContext");
    }

    public KsqlTransformer<Object, GenericRow> getMapper(
        final Function<QueryContext, ProcessingLogger> loggerFactory
    ) {
      return mapperFactory.apply(loggerFactory.apply(queryContext));
    }

    public <R> R visit(final TransformVisitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  public static class PredicateInfo implements TransformInfo {

    private final TransformFactory<KsqlTransformer<Object, Optional<GenericRow>>> predicate;
    private final QueryContext queryContext;

    PredicateInfo(
        final TransformFactory<KsqlTransformer<Object, Optional<GenericRow>>> predicate,
        final QueryContext queryContext
    ) {
      this.predicate = Objects.requireNonNull(predicate, "predicate");
      this.queryContext = requireNonNull(queryContext, "queryContext");
    }

    public KsqlTransformer<Object, Optional<GenericRow>> getPredicate(
        final Function<QueryContext, ProcessingLogger> loggerFactory
    ) {
      return predicate.apply(loggerFactory.apply(queryContext));
    }

    @Override
    public <R> R visit(final TransformVisitor<R> visitor) {
      return visitor.visit(this);
    }
  }
}

