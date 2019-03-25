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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.rules.ExternalResource;

public class TopicInjectorFixture extends ExternalResource  {

  private static final Schema SCHEMA = SchemaBuilder.struct()
      .field("F1", Schema.OPTIONAL_STRING_SCHEMA).build();

  private KsqlParser parser;
  private MutableMetaStore metaStore;
  private DefaultTopicInjector injector;
  private PreparedStatement<CreateStreamAsSelect> statement;
  private KsqlConfig ksqlConfig;
  private Map<String, Object> propertyOverrides;

  @Override
  protected void before() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    final FakeKafkaTopicClient fakeClient = new FakeKafkaTopicClient();
    injector = new DefaultTopicInjector(fakeClient, metaStore);
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        // meaningless configs
        "partitions", 15,
        "replicas", 15
    ));

    parser = new DefaultKsqlParser();
    propertyOverrides = new HashMap<>();

    final KsqlTopic ksqlTopic =
        new KsqlTopic("SOURCE", "source", new KsqlJsonTopicSerDe(), false);
    fakeClient.createTopic("source", Inject.SOURCE.partitions, Inject.SOURCE.replicas);
    metaStore.putSource(new KsqlStream<>(
        "",
        "SOURCE",
        SCHEMA,
        SCHEMA.fields().get(0),
        new MetadataTimestampExtractionPolicy(),
        ksqlTopic,
        Serdes.String()
    ));

    final KsqlTopic ksqlTopic2 =
        new KsqlTopic("SOURCE2", "source2", new KsqlJsonTopicSerDe(), false);
    fakeClient.createTopic("source2", Inject.SOURCE2.partitions, Inject.SOURCE2.replicas);
    metaStore.putSource(new KsqlStream<>(
        "",
        "SOURCE2",
        SCHEMA,
        SCHEMA.fields().get(0),
        new MetadataTimestampExtractionPolicy(),
        ksqlTopic2,
        Serdes.String()
    ));
  }

  @SuppressWarnings("unchecked")
  PreparedStatement<?> givenStatement(final String sql) {
    final PreparedStatement<?> preparedStatement =
        parser.prepare(parser.parse(sql).get(0), metaStore);
    if (preparedStatement.getStatement() instanceof CreateStreamAsSelect) {
      statement = (PreparedStatement<CreateStreamAsSelect>) preparedStatement;
    }
    return preparedStatement;
  }

  PreparedStatement<CreateStreamAsSelect> inject() {
    return injector.forStatement(statement, ksqlConfig, propertyOverrides);
  }

  PreparedStatement<?> inject(final PreparedStatement<?> statement) {
    return injector.forStatement(statement, ksqlConfig, propertyOverrides);
  }

  static Matcher<PreparedStatement<CreateStreamAsSelect>> hasTopicInfo(
      final String name,
      final int partitions,
      final short replicas
  ) {
    return new TypeSafeMatcher<PreparedStatement<CreateStreamAsSelect>>() {
      @Override
      protected boolean matchesSafely(PreparedStatement<CreateStreamAsSelect> item) {
        final Map<?, ?> properties = item.getStatement().getProperties();
        return properties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY)
            .equals(new StringLiteral(name))
            && properties.get(KsqlConstants.SINK_NUMBER_OF_PARTITIONS)
            .equals(new IntegerLiteral(partitions))
            && properties.get(KsqlConstants.SINK_NUMBER_OF_REPLICAS)
            .equals(new IntegerLiteral(replicas));
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(
            String.format("Expected (name = %s, partitions = %d, repliacs = %d)",
                name,
                partitions,
                replicas));
      }
    };
  }

  void givenInjects(final Inject... injects) {
    for (final Inject inject : injects) {
      switch (inject.type) {
        case WITH:
          final CreateStreamAsSelect csas = statement.getStatement();
          final Map<String, Expression> properties = new HashMap<>(csas.getProperties());
          if (inject.partitions != null) {
            properties.put(
                KsqlConstants.SINK_NUMBER_OF_PARTITIONS, new IntegerLiteral(inject.partitions));
          }
          if (inject.replicas!= null) {
            properties.put(
                KsqlConstants.SINK_NUMBER_OF_REPLICAS, new IntegerLiteral(inject.replicas));
          }
          final CreateStreamAsSelect copied = (CreateStreamAsSelect) csas.copyWith(properties);
          statement = PreparedStatement.of(SqlFormatter.formatSql(copied), copied);
          break;
        case OVERRIDES:
          if (inject.partitions != null) {
            propertyOverrides.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, inject.partitions);
          }
          if (inject.replicas != null) {
            propertyOverrides.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, inject.replicas);
          }
          break;
        case KSQL_CONFIG:
          final HashMap<String, Object> cfg = new HashMap<>();
          if (inject.partitions != null) {
            cfg.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, inject.partitions);
          }
          if (inject.replicas != null) {
            cfg.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, inject.replicas);
          }
          ksqlConfig = new KsqlConfig(cfg);
          break;
        case SOURCE:
        default:
          throw new IllegalArgumentException(inject.toString());
      }
    }
  }

  public TopicDescription getSource(final Inject source) {
      return new TopicDescription(
          "source",
          false,
          Collections.nCopies(source.partitions,
              new TopicPartitionInfo(
                  0,
                  null,
                  Collections.nCopies(source.replicas, new Node(0, "", 0)),
                  ImmutableList.of())
          )
      );
  }

  public KsqlConfig getKsqlConfig() {
    return ksqlConfig;
  }

  public Map<String, Expression> getWithClause() {
    return statement.getStatement().getProperties();
  }

  public Map<String, Object> getPropertyOverrides() {
    return propertyOverrides;
  }

  enum Type {
    WITH,
    OVERRIDES,
    KSQL_CONFIG,
    SOURCE
  }

  enum Inject {
    SOURCE(Type.SOURCE, 1, (short) 1),
    SOURCE2(Type.SOURCE, 12, (short) 12),

    WITH(Type.WITH, 2, (short) 2),
    OVERRIDES(Type.OVERRIDES, 3, (short) 3),
    KSQL_CONFIG(Type.KSQL_CONFIG, 4, (short) 4),

    WITH_P(Type.WITH, 5, null),
    OVERRIDES_P(Type.OVERRIDES, 6, null),
    KSQL_CONFIG_P(Type.KSQL_CONFIG, 7, null),

    WITH_R(Type.WITH, null, (short) 8),
    OVERRIDES_R(Type.OVERRIDES, null, (short) 9),
    KSQL_CONFIG_R(Type.KSQL_CONFIG, null, (short) 10),

    NO_WITH(Type.WITH, null, null),
    NO_OVERRIDES(Type.OVERRIDES, null, null),
    NO_CONFIG(Type.KSQL_CONFIG, null, null)
    ;

    final Type type;
    final Integer partitions;
    final Short replicas;

    Inject(final Type type, final Integer partitions, final Short replicas) {
      this.type = type;
      this.partitions = partitions;
      this.replicas = replicas;
    }
  }

  /**
   * Generates code for DefaultTopicInjectorTest$PrecedenceTest
   */
  public static void main(String[] args) {
    final List<Inject> withs = EnumSet.allOf(Inject.class)
        .stream().filter(i -> i.type == Type.WITH).collect(Collectors.toList());
    final List<Inject> overrides = EnumSet.allOf(Inject.class)
        .stream().filter(i -> i.type == Type.OVERRIDES).collect(Collectors.toList());
    final List<Inject> ksqlConfigs = EnumSet.allOf(Inject.class)
        .stream().filter(i -> i.type == Type.KSQL_CONFIG).collect(Collectors.toList());

    for (List<Inject> injects : Lists.cartesianProduct(withs, overrides, ksqlConfigs)) {
      // sort by precedence order
      injects = new ArrayList<>(injects);
      injects.sort(Comparator.comparing(i -> i.type));

      final Inject expectedPartitions =
          injects.stream().filter(i -> i.partitions != null).findFirst().orElse(Inject.SOURCE);
      final Inject expectedReplicas =
          injects.stream().filter(i -> i.replicas != null).findFirst().orElse(Inject.SOURCE);

      System.out.println(String.format("new Object[]{%-40s, %-15s, %-15s, new Inject[]{%s}},",
          "\"" + injects.toString().substring(1, injects.toString().length() - 1) + "\"",
          expectedPartitions,
          expectedReplicas,
          injects.stream().map(Objects::toString).collect(Collectors.joining(","))));
    }
  }
}
