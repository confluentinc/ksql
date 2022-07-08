/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.plan;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
    @Type(value = StreamAggregate.class, name = "streamAggregateV1"),
    @Type(value = StreamFilter.class, name = "streamFilterV1"),
    @Type(value = StreamFlatMap.class, name = "streamFlatMapV1"),
    @Type(value = StreamGroupByV1.class, name = "streamGroupByV1"),
    @Type(value = StreamGroupBy.class, name = "streamGroupByV2"),
    @Type(value = StreamGroupByKey.class, name = "streamGroupByKeyV1"),
    @Type(value = StreamSelect.class, name = "streamSelectV1"),
    @Type(value = StreamSelectKeyV1.class, name = "streamSelectKeyV1"),
    @Type(value = StreamSelectKey.class, name = "streamSelectKeyV2"),
    @Type(value = StreamSink.class, name = "streamSinkV1"),
    @Type(value = StreamSource.class, name = "streamSourceV1"),
    @Type(value = WindowedStreamSource.class, name = "windowedStreamSourceV1"),
    @Type(value = StreamStreamJoin.class, name = "streamStreamJoinV1"),
    @Type(value = StreamTableJoin.class, name = "streamTableJoinV1"),
    @Type(value = StreamWindowedAggregate.class, name = "streamWindowedAggregateV1"),
    @Type(value = TableSourceV1.class, name = "tableSourceV1"),
    @Type(value = TableSource.class, name = "tableSourceV2"),
    @Type(value = WindowedTableSource.class, name = "windowedTableSourceV1"),
    @Type(value = TableAggregate.class, name = "tableAggregateV1"),
    @Type(value = TableFilter.class, name = "tableFilterV1"),
    @Type(value = TableGroupByV1.class, name = "tableGroupByV1"),
    @Type(value = TableGroupBy.class, name = "tableGroupByV2"),
    @Type(value = TableSelect.class, name = "tableSelectV1"),
    @Type(value = TableSelectKey.class, name = "tableSelectKeyV1"),
    @Type(value = TableSink.class, name = "tableSinkV1"),
    @Type(value = TableSuppress.class, name = "tableSuppressV1"),
    @Type(value = TableTableJoin.class, name = "tableTableJoinV1"),
    @Type(value = ForeignKeyTableTableJoin.class, name = "fkTableTableJoinV1")
})
@Immutable
public interface ExecutionStep<S> {
  ExecutionStepProperties getProperties();

  @JsonIgnore
  List<ExecutionStep<?>> getSources();

  default S build(PlanBuilder planBuilder) {
    return build(planBuilder, extractPlanInfo(new PlanInfoExtractor()));
  }

  S build(PlanBuilder planBuilder, PlanInfo planInfo);

  PlanInfo extractPlanInfo(PlanInfoExtractor planInfoExtractor);

  /**
   * Checks whether or not this execution step can be safely replaced
   * with {@code other} without causing upgrade issues such as corrupted
   * state stores or repartition topics.
   *
   * <p>{@code validateUpgrade} checks compatibility by starting from the
   * parent and then traversing down the execution step tree. This means that
   * at any given point, the implementation can assume that the path above
   * the current node is upgrade compatible.
   *
   * <p>The implementation relies on two different types of nodes:
   *  <ol>
   *    <li><b>Enforcing Steps</b>: these steps are "stopping" points in the validation
   *    tree and will bring the {@code to} tree to match the enforcing step, skipping
   *    any passive steps that show up in between./li>
   *
   *    <li><b>Passive Steps</b>: these are steps that can be added/removed to topologies
   *    without causing any issues to upgrades. They will delegate their upgrade
   *    validation to their corresponding source nodes</li>
   *  </ol>
   * </p>
   *
   * <p>With this implementation strategy, we essentially have two pointers
   * into query plans: the source and the target. We traverse the trees advancing
   * the pointer in the source tree until we hit an enforcing step. The enforcing
   * step will advance the pointer until either the target tree has a matching,
   * valid enforcing step or there is an invalid step in between at which point
   * an exception will be propagated.
   *
   * <p>For example, consider the following two trees:
   * <pre>
   *   source (s): SINK -> SELECT -> SELECT_KEY -> SOURCE
   *   target (t): SINK -> FILTER -> SELECT_KEY -> SOURCE
   * </pre>
   * Within these trees, the {@code SINK}, {@code SELECT_KEY} and {@code SOURCE}
   * nodes are <i>enforcing</i> steps and the others are <i>passive</i> steps. The
   * algorithm would first compare the two SINK steps, then advance {@code s} until
   * the {@code s.SELECT_KEY} step. Since {@code t.FILTER} is passive, it can be skipped,
   * at which point the algorithm would hit another <i>enforcing</i> node ({@code t.SELECT_KEY}).
   * It would then compare those two steps and the two {@code SOURCE} steps to determine
   * whether or not the two trees are compatible. This essentially "reduces" the two trees
   * only to their enforcing nodes and makes sure that those are compatible with one
   * another.
   *
   * <p>The default implementation does not support upgrades, taking an "allowlist"
   * approach instead of a "denylist" approach so that we can guarantee that only
   * query upgrades that we've reasoned about and stamped with an approval will be
   * supported.
   *
   * @param to the execution step to upgrade to
   * @throws io.confluent.ksql.util.KsqlException if the upgrade is incompatible
   */
  default void validateUpgrade(@Nonnull ExecutionStep<?> to) {
    if (type() == StepType.PASSIVE) {
      to.validateUpgrade(getSources().get(0));
    } else {
      throw new IllegalStateException("ENFORCING steps must implement validateUpgrade");
    }
  }

  default StepType type() {
    throw new KsqlException("Upgrades not yet supported for " + getClass().getSimpleName());
  }

  /**
   * Utility method to help with {@link #validateUpgrade(ExecutionStep)} when
   * certain properties much match to be compatible. This helps because checkstyle
   * complains when code outside of an equals method has too many comparisons.
   *
   * @param that        the other execution step
   * @param properties  the properties to compare
   *
   * @throws io.confluent.ksql.util.KsqlException if any property does not match
   */
  default void mustMatch(ExecutionStep<?> that, List<Property> properties) {
    for (final Property property : properties) {
      final Object a = property.apply(this);
      final Object b = property.apply(that);
      if (!Objects.equals(a, b)) {
        throw new KsqlException(String.format(
            "Query is not upgradeable. Plan step of type %s must have matching %s. "
                + "Values differ: %s vs. %s",
            getClass().getSimpleName(), property.name, a, b));
      }
    }
  }

  /**
   * Used to help indicate which type of execution step is when comparing two
   * steps for upgrade compatibility. See {@link #validateUpgrade(ExecutionStep)} to
   * further understand how this enum is used in the comparison algorithm.
   */
  enum StepType {

    /**
     * An {@code ENFORCING} execution step requires that another compatible node
     * exists in the corresponding tree of the target upgrade path.
     */
    ENFORCING,

    /**
     * A {@code PASSIVE} execution step can be removed or added without restriction
     * in different physical plans.
     */
    PASSIVE
  }

  @Immutable
  class Property {

    private final String name;
    @EffectivelyImmutable
    private final Function<ExecutionStep<?>, ?> getter;

    Property(final String name, final Function<ExecutionStep<?>, ?> getter) {
      this.name = name;
      this.getter = getter;
    }

    Object apply(final ExecutionStep<?> step) {
      return getter.apply(step);
    }

  }

}
