/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.topic;

import static io.confluent.ksql.topic.TopicInjectorFixture.hasTopicInfo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.Lists;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.topic.TopicInjectorFixture.Inject;
import io.confluent.ksql.topic.TopicInjectorFixture.Type;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public class DefaultTopicInjectorTest {

  public static class Tests {
    @Rule
    public final TopicInjectorFixture fixture = new TopicInjectorFixture();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldInferTopicName() {
      // Given:
      fixture.givenStatement(
          "CREATE STREAM sink WITH (value_format='JSON') AS SELECT * FROM source;"
      );

      // When:
      final PreparedStatement<CreateStreamAsSelect> csas = fixture.inject();

      // Then:
      assertThat(
          csas.getStatement().getProperties(),
          hasEntry(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("SINK")));
    }

    @Test
    public void shouldUseTopicNameInWith() {
      // Given:
      fixture.givenStatement(
          "CREATE STREAM sink WITH (kafka_topic='topic', value_format='JSON') "
              + "AS SELECT * FROM source;"
      );

      // When:
      final PreparedStatement<CreateStreamAsSelect> csas = fixture.inject();

      // Then:
      assertThat(
          csas.getStatement().getProperties(),
          hasEntry(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic")));
    }

    @Test
    public void shouldInferFromLeftJoin() {
      // Given:
      fixture.givenStatement(
          "CREATE STREAM sink WITH (value_format='JSON') "
              + "AS SELECT * FROM source "
              + "JOIN source2 ON source.f1 = source2.f1;"
      );

      // When:
      final PreparedStatement<CreateStreamAsSelect> csas = fixture.inject();

      // Then:
      assertThat(csas, hasTopicInfo("SINK", Inject.SOURCE.partitions, Inject.SOURCE.replicas));
    }

    @Test
    public void testMalformedReplicaProperty() {
      // Given:
      fixture.givenStatement(
          "CREATE STREAM sink WITH (replicas='hi', value_format='JSON') AS SELECT * FROM source;"
      );

      // Expect:
      expectedException.expect(KsqlException.class);
      expectedException.expectMessage("Invalid number of replications in WITH clause: 'hi'");

      // When:
      fixture.inject();
    }

    @Test
    public void testMalformedReplicaPropertyNumber() {
      // Given:
      fixture.givenStatement(
          "CREATE STREAM sink WITH (replicas=.5, value_format='JSON') AS SELECT * FROM source;"
      );

      // Expect:
      expectedException.expect(KsqlException.class);
      expectedException.expectMessage("Invalid number of replications in WITH clause: 0.5");

      // When:
      fixture.inject();
    }

    @Test
    public void testMalformedReplicaPropertyInOverrides() {
      // Given:
      fixture.givenStatement(
          "CREATE STREAM sink WITH (value_format='JSON') AS SELECT * FROM source;"
      );
      fixture.propertyOverrides.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, "hi");

      // Expect:
      expectedException.expect(KsqlException.class);
      expectedException.expectMessage(
          "Invalid property override ksql.sink.replicas: hi "
              + "(all overrides: {ksql.sink.replicas=hi})");

      // When:
      fixture.inject();
    }

    @Test
    public void testMalformedPartitionsProperty() {
      // Given:
      fixture.givenStatement(
          "CREATE STREAM sink WITH (partitions='hi', value_format='JSON') AS SELECT * FROM source;"
      );

      // Expect:
      expectedException.expect(KsqlException.class);
      expectedException.expectMessage("Invalid number of partitions in WITH clause: 'hi'");

      // When:
      fixture.inject();
    }

    @Test
    public void testMalformedPartitionsPropertyNumber() {
      // Given:
      fixture.givenStatement(
          "CREATE STREAM sink WITH (partitions=.5, value_format='JSON') AS SELECT * FROM source;"
      );

      // Expect:
      expectedException.expect(KsqlException.class);
      expectedException.expectMessage("Invalid number of partitions in WITH clause: 0.5");

      // When:
      fixture.inject();
    }

    @Test
    public void testMalformedPartitionsPropertyInOverrides() {
      // Given:
      fixture.givenStatement(
          "CREATE STREAM sink WITH (value_format='JSON') AS SELECT * FROM source;"
      );
      fixture.propertyOverrides.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, "hi");

      // Expect:
      expectedException.expect(KsqlException.class);
      expectedException.expectMessage(
          "Invalid property override ksql.sink.partitions: hi "
              + "(all overrides: {ksql.sink.partitions=hi})");

      // When:
      fixture.inject();
    }

    @Test
    public void shouldDoNothingForNonCAS() {
      // Given:
      final PreparedStatement<?> statement = fixture.givenStatement(
          "SHOW TOPICS;"
      );

      // When:
      final PreparedStatement<?> injected = fixture.inject(statement);

      // Then:
      assertThat(injected, is(sameInstance(statement)));
    }
  }

  /**
   * This class tests all combinations of possible ways to get the topic
   * information. There are four place to get the information: with clause,
   * overrides, the ksql config and the source. For each place, there are
   * four options: both partitions/replicas are present, only partitions is
   * present, only replicas is present and nothing is present.
   */
  @RunWith(Parameterized.class)
  public static class PrecedenceTest {

    @Rule
    public final TopicInjectorFixture fixture = new TopicInjectorFixture();

    @Parameters(name="given {0} -> expect({1},{2})")
    public static Iterable<Object[]> data() {
      final List<Inject> withs = EnumSet.allOf(Inject.class)
          .stream().filter(i -> i.type == Type.WITH).collect(Collectors.toList());
      final List<Inject> overrides = EnumSet.allOf(Inject.class)
          .stream().filter(i -> i.type == Type.OVERRIDES).collect(Collectors.toList());
      final List<Inject> ksqlConfigs = EnumSet.allOf(Inject.class)
          .stream().filter(i -> i.type == Type.KSQL_CONFIG).collect(Collectors.toList());

      final List<Object[]> parameters = new ArrayList<>();
      for (List<Inject> injects : Lists.cartesianProduct(withs, overrides, ksqlConfigs)) {
        // sort by precedence order
        injects = new ArrayList<>(injects);
        injects.sort(Comparator.comparing(i -> i.type));

        final Inject expectedPartitions =
            injects.stream().filter(i -> i.partitions != null).findFirst().orElse(Inject.SOURCE);
        final Inject expectedReplicas =
            injects.stream().filter(i -> i.replicas != null).findFirst().orElse(Inject.SOURCE);

        parameters.add(new Object[]{
            injects.toString(),
            expectedPartitions,
            expectedReplicas,
            injects.toArray(new Inject[]{})});
      }

      return parameters;
    }

    @Parameter
    public String description;

    @Parameter(1)
    public Inject expectedPartitions;

    @Parameter(2)
    public Inject expectedReplicas;

    @Parameter(3)
    public Inject[] injects;

    @Test
    public void test() {
      // Given:
      fixture.givenStatement(
          "CREATE STREAM sink WITH (value_format='JSON') AS SELECT * FROM source;");
      fixture.givenInjects(injects);

      // When:
      final PreparedStatement<CreateStreamAsSelect> csas = fixture.inject();

      // Then:
      assertThat(csas,
          hasTopicInfo("SINK", expectedPartitions.partitions, expectedReplicas.replicas));
    }
  }

}
