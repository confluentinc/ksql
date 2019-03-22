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

import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.KSQL_CONFIG;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.KSQL_CONFIG_P;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.KSQL_CONFIG_R;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.NO_CONFIG;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.NO_OVERRIDES;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.NO_WITH;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.OVERRIDES;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.OVERRIDES_P;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.OVERRIDES_R;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.SOURCE;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.WITH;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.WITH_P;
import static io.confluent.ksql.topic.TopicInjectorFixture.Inject.WITH_R;
import static io.confluent.ksql.topic.TopicInjectorFixture.hasTopicInfo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import com.google.common.collect.Lists;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.topic.TopicInjectorFixture.Inject;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
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
      final Object[][] data = new Object[][]{
          // THIS LIST WAS GENERATED BY RUNNING TopicInjectorFixture#main
          //
          // DESCRIPTION                             EXPECTED:  [PARTITIONS       REPLICAS]        GIVEN: OVERRIDES
          new Object[]{"WITH, OVERRIDES, KSQL_CONFIG"          , WITH           , WITH           , new Inject[]{WITH,OVERRIDES,KSQL_CONFIG}},
          new Object[]{"WITH, OVERRIDES, KSQL_CONFIG_P"        , WITH           , WITH           , new Inject[]{WITH,OVERRIDES,KSQL_CONFIG_P}},
          new Object[]{"WITH, OVERRIDES, KSQL_CONFIG_R"        , WITH           , WITH           , new Inject[]{WITH,OVERRIDES,KSQL_CONFIG_R}},
          new Object[]{"WITH, OVERRIDES, NO_CONFIG"            , WITH           , WITH           , new Inject[]{WITH,OVERRIDES,NO_CONFIG}},
          new Object[]{"WITH, OVERRIDES_P, KSQL_CONFIG"        , WITH           , WITH           , new Inject[]{WITH,OVERRIDES_P,KSQL_CONFIG}},
          new Object[]{"WITH, OVERRIDES_P, KSQL_CONFIG_P"      , WITH           , WITH           , new Inject[]{WITH,OVERRIDES_P,KSQL_CONFIG_P}},
          new Object[]{"WITH, OVERRIDES_P, KSQL_CONFIG_R"      , WITH           , WITH           , new Inject[]{WITH,OVERRIDES_P,KSQL_CONFIG_R}},
          new Object[]{"WITH, OVERRIDES_P, NO_CONFIG"          , WITH           , WITH           , new Inject[]{WITH,OVERRIDES_P,NO_CONFIG}},
          new Object[]{"WITH, OVERRIDES_R, KSQL_CONFIG"        , WITH           , WITH           , new Inject[]{WITH,OVERRIDES_R,KSQL_CONFIG}},
          new Object[]{"WITH, OVERRIDES_R, KSQL_CONFIG_P"      , WITH           , WITH           , new Inject[]{WITH,OVERRIDES_R,KSQL_CONFIG_P}},
          new Object[]{"WITH, OVERRIDES_R, KSQL_CONFIG_R"      , WITH           , WITH           , new Inject[]{WITH,OVERRIDES_R,KSQL_CONFIG_R}},
          new Object[]{"WITH, OVERRIDES_R, NO_CONFIG"          , WITH           , WITH           , new Inject[]{WITH,OVERRIDES_R,NO_CONFIG}},
          new Object[]{"WITH, NO_OVERRIDES, KSQL_CONFIG"       , WITH           , WITH           , new Inject[]{WITH,NO_OVERRIDES,KSQL_CONFIG}},
          new Object[]{"WITH, NO_OVERRIDES, KSQL_CONFIG_P"     , WITH           , WITH           , new Inject[]{WITH,NO_OVERRIDES,KSQL_CONFIG_P}},
          new Object[]{"WITH, NO_OVERRIDES, KSQL_CONFIG_R"     , WITH           , WITH           , new Inject[]{WITH,NO_OVERRIDES,KSQL_CONFIG_R}},
          new Object[]{"WITH, NO_OVERRIDES, NO_CONFIG"         , WITH           , WITH           , new Inject[]{WITH,NO_OVERRIDES,NO_CONFIG}},
          new Object[]{"WITH_P, OVERRIDES, KSQL_CONFIG"        , WITH_P         , OVERRIDES      , new Inject[]{WITH_P,OVERRIDES,KSQL_CONFIG}},
          new Object[]{"WITH_P, OVERRIDES, KSQL_CONFIG_P"      , WITH_P         , OVERRIDES      , new Inject[]{WITH_P,OVERRIDES,KSQL_CONFIG_P}},
          new Object[]{"WITH_P, OVERRIDES, KSQL_CONFIG_R"      , WITH_P         , OVERRIDES      , new Inject[]{WITH_P,OVERRIDES,KSQL_CONFIG_R}},
          new Object[]{"WITH_P, OVERRIDES, NO_CONFIG"          , WITH_P         , OVERRIDES      , new Inject[]{WITH_P,OVERRIDES,NO_CONFIG}},
          new Object[]{"WITH_P, OVERRIDES_P, KSQL_CONFIG"      , WITH_P         , KSQL_CONFIG    , new Inject[]{WITH_P,OVERRIDES_P,KSQL_CONFIG}},
          new Object[]{"WITH_P, OVERRIDES_P, KSQL_CONFIG_P"    , WITH_P         , SOURCE         , new Inject[]{WITH_P,OVERRIDES_P,KSQL_CONFIG_P}},
          new Object[]{"WITH_P, OVERRIDES_P, KSQL_CONFIG_R"    , WITH_P         , KSQL_CONFIG_R  , new Inject[]{WITH_P,OVERRIDES_P,KSQL_CONFIG_R}},
          new Object[]{"WITH_P, OVERRIDES_P, NO_CONFIG"        , WITH_P         , SOURCE         , new Inject[]{WITH_P,OVERRIDES_P,NO_CONFIG}},
          new Object[]{"WITH_P, OVERRIDES_R, KSQL_CONFIG"      , WITH_P         , OVERRIDES_R    , new Inject[]{WITH_P,OVERRIDES_R,KSQL_CONFIG}},
          new Object[]{"WITH_P, OVERRIDES_R, KSQL_CONFIG_P"    , WITH_P         , OVERRIDES_R    , new Inject[]{WITH_P,OVERRIDES_R,KSQL_CONFIG_P}},
          new Object[]{"WITH_P, OVERRIDES_R, KSQL_CONFIG_R"    , WITH_P         , OVERRIDES_R    , new Inject[]{WITH_P,OVERRIDES_R,KSQL_CONFIG_R}},
          new Object[]{"WITH_P, OVERRIDES_R, NO_CONFIG"        , WITH_P         , OVERRIDES_R    , new Inject[]{WITH_P,OVERRIDES_R,NO_CONFIG}},
          new Object[]{"WITH_P, NO_OVERRIDES, KSQL_CONFIG"     , WITH_P         , KSQL_CONFIG    , new Inject[]{WITH_P,NO_OVERRIDES,KSQL_CONFIG}},
          new Object[]{"WITH_P, NO_OVERRIDES, KSQL_CONFIG_P"   , WITH_P         , SOURCE         , new Inject[]{WITH_P,NO_OVERRIDES,KSQL_CONFIG_P}},
          new Object[]{"WITH_P, NO_OVERRIDES, KSQL_CONFIG_R"   , WITH_P         , KSQL_CONFIG_R  , new Inject[]{WITH_P,NO_OVERRIDES,KSQL_CONFIG_R}},
          new Object[]{"WITH_P, NO_OVERRIDES, NO_CONFIG"       , WITH_P         , SOURCE         , new Inject[]{WITH_P,NO_OVERRIDES,NO_CONFIG}},
          new Object[]{"WITH_R, OVERRIDES, KSQL_CONFIG"        , OVERRIDES      , WITH_R         , new Inject[]{WITH_R,OVERRIDES,KSQL_CONFIG}},
          new Object[]{"WITH_R, OVERRIDES, KSQL_CONFIG_P"      , OVERRIDES      , WITH_R         , new Inject[]{WITH_R,OVERRIDES,KSQL_CONFIG_P}},
          new Object[]{"WITH_R, OVERRIDES, KSQL_CONFIG_R"      , OVERRIDES      , WITH_R         , new Inject[]{WITH_R,OVERRIDES,KSQL_CONFIG_R}},
          new Object[]{"WITH_R, OVERRIDES, NO_CONFIG"          , OVERRIDES      , WITH_R         , new Inject[]{WITH_R,OVERRIDES,NO_CONFIG}},
          new Object[]{"WITH_R, OVERRIDES_P, KSQL_CONFIG"      , OVERRIDES_P    , WITH_R         , new Inject[]{WITH_R,OVERRIDES_P,KSQL_CONFIG}},
          new Object[]{"WITH_R, OVERRIDES_P, KSQL_CONFIG_P"    , OVERRIDES_P    , WITH_R         , new Inject[]{WITH_R,OVERRIDES_P,KSQL_CONFIG_P}},
          new Object[]{"WITH_R, OVERRIDES_P, KSQL_CONFIG_R"    , OVERRIDES_P    , WITH_R         , new Inject[]{WITH_R,OVERRIDES_P,KSQL_CONFIG_R}},
          new Object[]{"WITH_R, OVERRIDES_P, NO_CONFIG"        , OVERRIDES_P    , WITH_R         , new Inject[]{WITH_R,OVERRIDES_P,NO_CONFIG}},
          new Object[]{"WITH_R, OVERRIDES_R, KSQL_CONFIG"      , KSQL_CONFIG    , WITH_R         , new Inject[]{WITH_R,OVERRIDES_R,KSQL_CONFIG}},
          new Object[]{"WITH_R, OVERRIDES_R, KSQL_CONFIG_P"    , KSQL_CONFIG_P  , WITH_R         , new Inject[]{WITH_R,OVERRIDES_R,KSQL_CONFIG_P}},
          new Object[]{"WITH_R, OVERRIDES_R, KSQL_CONFIG_R"    , SOURCE         , WITH_R         , new Inject[]{WITH_R,OVERRIDES_R,KSQL_CONFIG_R}},
          new Object[]{"WITH_R, OVERRIDES_R, NO_CONFIG"        , SOURCE         , WITH_R         , new Inject[]{WITH_R,OVERRIDES_R,NO_CONFIG}},
          new Object[]{"WITH_R, NO_OVERRIDES, KSQL_CONFIG"     , KSQL_CONFIG    , WITH_R         , new Inject[]{WITH_R,NO_OVERRIDES,KSQL_CONFIG}},
          new Object[]{"WITH_R, NO_OVERRIDES, KSQL_CONFIG_P"   , KSQL_CONFIG_P  , WITH_R         , new Inject[]{WITH_R,NO_OVERRIDES,KSQL_CONFIG_P}},
          new Object[]{"WITH_R, NO_OVERRIDES, KSQL_CONFIG_R"   , SOURCE         , WITH_R         , new Inject[]{WITH_R,NO_OVERRIDES,KSQL_CONFIG_R}},
          new Object[]{"WITH_R, NO_OVERRIDES, NO_CONFIG"       , SOURCE         , WITH_R         , new Inject[]{WITH_R,NO_OVERRIDES,NO_CONFIG}},
          new Object[]{"NO_WITH, OVERRIDES, KSQL_CONFIG"       , OVERRIDES      , OVERRIDES      , new Inject[]{NO_WITH,OVERRIDES,KSQL_CONFIG}},
          new Object[]{"NO_WITH, OVERRIDES, KSQL_CONFIG_P"     , OVERRIDES      , OVERRIDES      , new Inject[]{NO_WITH,OVERRIDES,KSQL_CONFIG_P}},
          new Object[]{"NO_WITH, OVERRIDES, KSQL_CONFIG_R"     , OVERRIDES      , OVERRIDES      , new Inject[]{NO_WITH,OVERRIDES,KSQL_CONFIG_R}},
          new Object[]{"NO_WITH, OVERRIDES, NO_CONFIG"         , OVERRIDES      , OVERRIDES      , new Inject[]{NO_WITH,OVERRIDES,NO_CONFIG}},
          new Object[]{"NO_WITH, OVERRIDES_P, KSQL_CONFIG"     , OVERRIDES_P    , KSQL_CONFIG    , new Inject[]{NO_WITH,OVERRIDES_P,KSQL_CONFIG}},
          new Object[]{"NO_WITH, OVERRIDES_P, KSQL_CONFIG_P"   , OVERRIDES_P    , SOURCE         , new Inject[]{NO_WITH,OVERRIDES_P,KSQL_CONFIG_P}},
          new Object[]{"NO_WITH, OVERRIDES_P, KSQL_CONFIG_R"   , OVERRIDES_P    , KSQL_CONFIG_R  , new Inject[]{NO_WITH,OVERRIDES_P,KSQL_CONFIG_R}},
          new Object[]{"NO_WITH, OVERRIDES_P, NO_CONFIG"       , OVERRIDES_P    , SOURCE         , new Inject[]{NO_WITH,OVERRIDES_P,NO_CONFIG}},
          new Object[]{"NO_WITH, OVERRIDES_R, KSQL_CONFIG"     , KSQL_CONFIG    , OVERRIDES_R    , new Inject[]{NO_WITH,OVERRIDES_R,KSQL_CONFIG}},
          new Object[]{"NO_WITH, OVERRIDES_R, KSQL_CONFIG_P"   , KSQL_CONFIG_P  , OVERRIDES_R    , new Inject[]{NO_WITH,OVERRIDES_R,KSQL_CONFIG_P}},
          new Object[]{"NO_WITH, OVERRIDES_R, KSQL_CONFIG_R"   , SOURCE         , OVERRIDES_R    , new Inject[]{NO_WITH,OVERRIDES_R,KSQL_CONFIG_R}},
          new Object[]{"NO_WITH, OVERRIDES_R, NO_CONFIG"       , SOURCE         , OVERRIDES_R    , new Inject[]{NO_WITH,OVERRIDES_R,NO_CONFIG}},
          new Object[]{"NO_WITH, NO_OVERRIDES, KSQL_CONFIG"    , KSQL_CONFIG    , KSQL_CONFIG    , new Inject[]{NO_WITH,NO_OVERRIDES,KSQL_CONFIG}},
          new Object[]{"NO_WITH, NO_OVERRIDES, KSQL_CONFIG_P"  , KSQL_CONFIG_P  , SOURCE         , new Inject[]{NO_WITH,NO_OVERRIDES,KSQL_CONFIG_P}},
          new Object[]{"NO_WITH, NO_OVERRIDES, KSQL_CONFIG_R"  , SOURCE         , KSQL_CONFIG_R  , new Inject[]{NO_WITH,NO_OVERRIDES,KSQL_CONFIG_R}},
          new Object[]{"NO_WITH, NO_OVERRIDES, NO_CONFIG"      , SOURCE         , SOURCE         , new Inject[]{NO_WITH,NO_OVERRIDES,NO_CONFIG}}
      };
      return Lists.newArrayList(data);
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
