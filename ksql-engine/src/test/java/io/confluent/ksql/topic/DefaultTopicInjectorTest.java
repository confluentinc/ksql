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
